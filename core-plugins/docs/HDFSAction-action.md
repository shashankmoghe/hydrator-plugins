# HDFS Action


Description
-----------
Moves a file or files within an HDFS cluster.


Use Case
--------
This action can be used when a file or files need to be moved to a new location in an HDFS cluster, 
often required when archiving files.


Properties
----------
**sourcePath:** The full HDFS path of the file/directory that the user wants to move. In the case of a directory,
if fileRegex is set, then all files in the directory matching the wildcard regex will be moved. Otherwise,
all files in the directory will be moved. Ex: hdfs://hostname/tmp

**destPath:** The full HDFS destination path in the same cluster where the user wants to move the file(s) specified
in the sourcePath. It is assumed that the path is valid. For moving a file, this means that the parent directories
exist. For moving multiple files in a directory or an entire directory, the path to the desired directory location
should be passed and that directory and all parent directories should already exist.

**fileRegex:** The wildcard regex to filter what kind of files in the directory to move.

**continueOnError:** Indicates whether or not the pipeline should stop if the file move process fails.


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
        },
        "properties": {
          "sourcePath": "hdfs://123.23.12.4344:10000/source/path",
          "destPath": "hdfs://123.23.12.4344:10000/dest/path",
          "fileRegex": ".*\.txt",
          "continueOnError": "false"
        }
    }
