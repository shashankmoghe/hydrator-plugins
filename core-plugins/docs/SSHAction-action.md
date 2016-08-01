# SSH Action


Description
-----------
Establishes an SSH connection with remote machine to execute command on that machine.


Use Case
--------
This action can be used when data needs to be copied to HDFS from a remote server before the pipeline starts.
It can also be used to move data for archiving before the pipeline starts.


Properties
----------

**host:** The host name of the remote machine where the command needs to be executed.

**port:** The port used to make SSH connection. Defaults to 22.

**user:** The username used to connect to host.

**privateKeyFile:** The file path to Private key.

**password:** The password associated with private key.

**command:** The command to be executed on the remote host. This includes the filepath of the script and any arguments
needing to be passed.

**output:** The key name to store any important output from the script run through the workflow token.

Example
-------
This example runs a script on demo@example.com:

    {
        "name": "SSHAction",
          "plugin": {
            "name": "SSHAction",
            "type": "action",
            "label": "SSHAction",
            "artifact": {
              "name": "core-plugins",
              "version": "1.4.0-SNAPSHOT",
              "scope": "SYSTEM"
          },
          "properties": {
              "host": "example.com",
              "password": "pass",
              "port": "22",
              "user": "demo",
              "privateKeyFile": "/Users/Jose/.ssh/id_rsa",
              "command": "~/scripts/remoteScript.sh"
            }
          }
    }
