.. _encrypting_passwords:

Encrypt configuration values
============================

PipelineWise can decrypt Ansible Vault scalar values embedded in project YAML.
Vault encryption protects stored configuration but does not replace access
control, secret rotation, or runtime credential isolation.


Create the vault password file
------------------------------

Store the vault password outside the project and restrict it before writing the
secret:

.. code-block:: bash

   umask 077
   printf '%s\n' '<vault-password>' > /secure/path/pipelinewise-vault-password

Keep a recoverable copy in an approved secret manager. Anyone with this file and
the encrypted YAML can recover every protected value.


Encrypt a value
---------------

.. code-block:: bash

   pipelinewise encrypt_string \
     --secret /secure/path/pipelinewise-vault-password \
     --string '<value>'

The plaintext argument can be visible in shell history and process listings.
Run the command only in an appropriately isolated session and clear any retained
history according to local security policy.

Copy the complete ``!vault`` result into YAML:

.. code-block:: yaml

   db_conn:
     user: "pipelinewise"
     password: !vault |
       $ANSIBLE_VAULT;1.1;AES256
       <encrypted-data>


Import encrypted YAML
---------------------

.. code-block:: bash

   pipelinewise import_config \
     --dir <project> \
     --secret /secure/path/pipelinewise-vault-password

Missing or incorrect vault credentials fail import before a pipeline is updated.
Do not copy the vault password into the project, image, logs, or command output.


Rotation and recovery
---------------------

To rotate the vault password, decrypt and re-encrypt every affected value in a
controlled change, validate the project with the new password, then retire the
old password. Rotate the underlying database or API credential separately.

If the vault password is lost, PipelineWise cannot recover the plaintext. Replace
the source credentials and update the YAML rather than bypassing validation or
editing generated connector JSON.
