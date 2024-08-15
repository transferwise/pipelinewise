import os
import inspect
import importlib
import importlib.machinery
import sys
import subprocess

from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from google.protobuf.json_format import MessageToDict
from google.protobuf.json_format import MessageToJson

from tap_kafka.errors import ProtobufCompilerException


# pylint: disable=R0903
class ProtobufDictDeserializer(ProtobufDeserializer):
    """
    Deserializes a Python dict object from protobuf
    """
    def __call__(self, value, ctx):
        msg =  super().__call__(value, ctx)
        return MessageToDict(msg,
                             preserving_proto_field_name=True,
                             including_default_value_fields=True)

def topic_name_to_protoc_output_name(topic: str) -> str:
    """Convert topic name to the file name that protoc is generating"""
    return topic.replace('-', '_').replace('.', '_')

# pylint: disable=R0914
def proto_to_message_type(schema: str, protobuf_classes_dir: str, topic: str):
    """Compile a protobuf schema to python class and load it dynamically"""
    mod_name = f"proto_message_{topic_name_to_protoc_output_name(topic)}"
    proto_name = f"{mod_name}.proto"
    module_name = f"{mod_name}_pb2"
    module_filename = f"{module_name}.py"

    if not os.path.exists(os.path.expanduser(protobuf_classes_dir)):
        os.makedirs(protobuf_classes_dir, exist_ok=True)

    schema_filename = os.path.join(protobuf_classes_dir, proto_name)
    with open(schema_filename, 'w+') as schema_f:
        schema_f.write(schema)
        schema_f.flush()

    # Compile schema to python class by protoc
    command = f"{sys.executable} -m grpc_tools.protoc -I {protobuf_classes_dir} --python_out={protobuf_classes_dir} {proto_name}"
    try:
        subprocess.run(command.split(), check=True, stdout=subprocess.PIPE, env=os.environ.copy())
    except subprocess.CalledProcessError as exc:
        raise ProtobufCompilerException(f"Cannot generate proto class: {exc}")

    # Load the class dynamically
    mod_name_with_pkg = f"tap_kafka.{mod_name}"
    loader = importlib.machinery.SourceFileLoader(
        mod_name_with_pkg, os.path.join(protobuf_classes_dir, module_filename)
    )
    spec = importlib.util.spec_from_loader(mod_name_with_pkg, loader)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    loader.exec_module(mod)

    # Get the generated class
    for name, obj in inspect.getmembers(mod):
        if inspect.isclass(obj) and obj.__module__ == module_name:
            return obj

    return None
