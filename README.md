# NeoFinderToES

## Prerequisites

* Java 8

## Usage

```
neofindertoes [options] FILE_OR_DIRECTORY1 [FILE_OR_DIRECTORY2 [FILE_OR_DIRECTORY3] ...]
Options:
 -a,--address <ADDRESS>       the address of the elasticsearch index
                              (omitting this the local loopback address
                              will be used)
 -c,--catalog                 read cdfinder/neofinder catalog files
 -e,--esclustername <NAME>    the name of the elasticsearch cluster
                              (omitting this the default name
                              'elasticsearch' will be used)
 -h,--help                    print this message
 -i,--indexname <NAME>        the name of the elasticsearch index
                              (omitting this the name 'marbildertivoli'
                              will be used)
 -m,--mimeType <STRATEGY>     the mime type fetch strategy to use:
                              0: no mime type information is fetched
                              (default)
                              1: mime type is 'guessed' based on file
                              extension
                              2: mime type is detected by inspecting the
                              file (most accurate but slow)
 -n,--newindex                create a new elasticsearch index (if an old
                              one with the same name exists it will be
                              deleted
 -t,--threads <MAX_THREADS>   the maximum number of threads used for
                              reading (the default value is the number of
                              available CPUs/Cores)
 -v,--verbose                 show JSON objects that are added to the
                              index
```

## Exit codes
```
0 - success 
1 - unrecognized command line option 
2 - failed to parse command line 
3 - failed to create elasticsearch index 
4 - file or directory does not exist 
5 - io exception 
6 - elasticsearch host not found
7 - could not connect to elasticsearch cluster
```