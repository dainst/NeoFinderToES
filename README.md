# NeoFinderToES

## Prerequisites

* Java 8

## Usage

```
usage: neofindertoes [options] FILE_OR_DIRECTORY1 [FILE_OR_DIRECTORY2
                     [FILE_OR_DIRECTORY3] ...]
Options:
 -A,--autocorrect             enables auto correction:
                              - try to fix lines with less columns than
                              the header specifies
                              - if only one date column could be parsed
                              assign this value to both date fields
                              (only works with option -c)
 -a,--address <ADDRESS>       the address of the elasticsearch index
                              (omitting this the local loopback address
                              will be used)
 -c,--catalog                 parse and import cdfinder/neofinder catalog
                              files
 -e,--esclustername <NAME>    the name of the elasticsearch cluster
                              (omitting this the default name
                              'elasticsearch' will be used)
 -h,--help                    print this message
 -i,--indexname <NAME>        the name of the elasticsearch index
                              (omitting this the name 'marbildertivoli'
                              will be used)
 -I,--ignore <FIELDLIST>      the field or fields to ignore potentially
                              invalid data for
                              if multiple fields are specified they must
                              be comma separated
                              (use with care as this may create records
                              that will miss the specified fields)
                              (only works with option -c)
 -m,--mimeType <STRATEGY>     the mime type fetch strategy to use:
                              0: no mime type information is fetched
                              (default)
                              1: mime type is 'guessed' based on file
                              extension
                              2: mime type is detected by inspecting the
                              file (most accurate but slow)
                              (only works without option -c)
 -n,--newindex                create a new elasticsearch index
                              (if an old one with the same name exists it
                              will be deleted)
 -t,--threads <MAX_THREADS>   the maximum number of threads used for file
                              system reading
                              (the default value is the number of
                              available CPU cores)
                              (only works without option -c)
 -v,--verbose                 show JSON objects that are added to the
                              index
```

## Exit codes
```
0 - success
1 - unrecognized command line option
2 - failed to parse command line
3 - failed to create elasticsearch index
4 - unknown field given for -I
6 - elasticsearch host not found
7 - could not connect to elasticsearch cluster
```

## Build

Prerequisites:
* Maven

```
mvn clean package
```
