# s3-to-redshift

A utility to load files from S3 to Redshift.

## Usage

```sh
$ lein run -m s3-to-redshift.core config.edn
```

This will find data files in S3 as specified by the config file; check which ones have not yet been loaded; group them in batches and create corresponding manifest files; execute redshift's COPY commands; and mark the files as loaded.

You need to first create a table to track loaded files:

```SQL
create table s3_loaded_file(table_name varchar, url varchar(2048), create_time timestamp default sysdate, primary key (table_name, url));
```

The table name can be different, you just have to specify it in the config file.

See the [example config](config.sample.edn), hopefully self-explanatory.


## License

Copyright © 2015 Yummly

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
