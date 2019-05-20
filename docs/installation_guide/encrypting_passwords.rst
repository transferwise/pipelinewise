.. _encrypting_passwords:

Encrypting Passwords
====================

PipelineWise has a built-in feature that allows you to keep sensitive data such as passwords or keys
in encrypted format, rather than as plaintext in the project YAML files. These encrypted strings can
then be distributed or placed in source control.

PipelineWise is using the `Ansible Vault <https://docs.ansible.com/ansible/latest/user_guide/vault.html>`_
python libraries to encrypt and decrypt strings. The default cipher is AES (which is shared-secret based).

1. To encrypt data, first you need to create file with a secret password. In this example we will create
a ``vault-password.txt`` file from the command that you will keep in a safe place:

.. code-block:: bash

    $ echo "M@st3rP@ssw0rd" > vault-password.txt


2. Now you can encrypt the sensitive strings in the PipelineWise project that is usually database passwords
or other data source or destination credentials that you don't want to place in source control as
plain texts. To encrypt a string run:

.. code-block:: bash

    $ pipelinewise encrypt_string --secret vault-password.txt --string "This is a string to encrypt"
    !vault |
              $ANSIBLE_VAULT;1.1;AES256
              31376164363334663765396232363562653463613862333163396565396239336134636261326137
              3561303661636161663337333564316463653230623436650a333639313136393930656232393334
              34303232656430303664393238656633336333663965303333643134326239363538646237356130
              3662383632313763650a633664633665646238373861356430336536616239343535616231653161
              37376232313836613939636434303863333035653534633533333739303137323034
    Encryption successful


3. Now you can copy the output of the previous step  and use it in any YAML file instead of
plain passwords. For example in a ``tap_mysql.yml`` file the ``db_conn`` section will look like this:

.. code-block:: bash

  db_conn:
    host: "mysql_source_database"
    port: 3306
    user: "pgninja_replica"
    password: !vault |
          $ANSIBLE_VAULT;1.1;AES256
          31376164363334663765396232363562653463613862333163396565396239336134636261326137
          3561303661636161663337333564316463653230623436650a333639313136393930656232393334
          34303232656430303664393238656633336333663965303333643134326239363538646237356130
          3662383632313763650a633664633665646238373861356430336536616239343535616231653161
          37376232313836613939636434303863333035653534633533333739303137323034
    dbname: "fx"

4. When importing the project YAML files into PipelineWise you will need to provide
the path to the file with the password (the one that you created in the first step) using the
``--secret`` command line option. For example if you have a sample project in
``pipelinewise_samples`` you will need to run:

.. code-block:: bash

    $ pipelinewise import_config --dir pipelinewise_samples --secret vault-password.txt


------------


**Tip:**
Further details about creating and importing project please check the :ref:`creating_pipelines`
section.

