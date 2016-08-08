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
**sourcePath:** The full HDFS path of the file or directory that is to be moved. In the case of a directory, if
fileRegex is set, then only files in the source directory matching the wildcard regex will be moved.
Otherwise, all files in the directory will be moved. For example: `hdfs://hostname/tmp`.

**destPath:** The valid, full HDFS destination path in the same cluster where the file or files are to be moved.
For moving a single file, this means that all parent directories must already exist.For moving multiple files in a
directory or an entire directory, the path to the desired directory is passed and that directory and all parent
directories must already exist.

**fileRegex:** Wildcard regular expression to filter the files in the source directory that will be moved.

**continueOnError:** Indicates if the pipeline should continue if the move process fails.


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
            "fileRegex": ".*\.txt",
            "continueOnError": "false"
        }
    }
}
