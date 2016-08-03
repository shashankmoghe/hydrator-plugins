# HDFS Action


Description
-----------
Moves file(s) within HDFS.


Use Case
--------
This action can be used when files needs to be moved to a new location in HDFS for the purpose of archiving,
for example.


Properties
----------
**sourcePath:** The full HDFS path of the file/directory that the user wants to move.

**destPath:** The full HDFS destination path where the user wants to move the file(s) in sourcePath.

**fileRegex:** The wildcard regex to filter what kind of files in the directory to move.


Example
-------
This example moves a file from /source/path to /dest/path:

    {
        "name": "HDFSAction",
          "plugin": {
            "name": "HDFSAction",
            "type": "action",
            "label": "HDFSAction",
            "artifact": {
              "name": "core-plugins",
              "version": "1.4.0-SNAPSHOT",
              "scope": "SYSTEM"
          },
          "properties": {
              "sourcePath": "hdfs://123.23.12.4344:10000/source/path",
              "destPath": "hdfs://123.23.12.4344:10000/dest/path",
              "fileRegex": ".*\.txt"
            }
          }
    }
