# s3stream - Merge lines from S3 objects to stdout

---

*Read large amounts of text files from Amazon S3 line-by-line and dump their
merged contents to standard output so you can use a pipeline of Unix/Linux
filters (*e.g.* `sed`, `grep`, `gzip`, `jq`, AWS CLI, *etc.*) to process them.*

---

# Features

- Read S3 objects line-by-line and write their lines to standard output.  
- Download and merge up to 32 S3 objects in parallel.
- Automatically unzip GZIP-ed text files.

# Install

```sh
$ go get github.com/gogama/aws-s3-stream
```

# Build

```sh
$ go build -o s3stream
```

# Run

Read a single S3 object and dump its lines to standard output.

```sh
$ AWS_REGION=<region> ./s3stream s3://your-bucket/path/to/object 
```

Read multiple objects and dump them to stdout.

```sh
$ AWS_REGION=<region> ./s3stream -p s3://your-bucket/some/prefix obj1 obj2 obj3 obj4 obj5
```

Read all objects under a prefix with maximum concurrency, dump them to stdout,
and GZIP and upload the merged object back to S3 (uses AWS CLI).

```sh
$ aws --region <region> s3 ls --recursive input-bucket/prefix |
    awk '{print $4}' |
    AWS_REGION=<region> ./s3stream -p s3://input-bucket/prefix -c 32 |
    gzip --best |
    aws s3 cp - s3://output-bucket/merged.gz
```

# License

MIT

# Backlog

- Profile performance, assess bottlenecks and whether increasing/reducing
  parallelism would help in places.
- Support other ways to provide AWS region other `AWS_REGION` environment variable.
- Add option, on by default, to detect and ignore objects that aren't text.
- Add option, off by default, to unpack and look inside archives.
- Support S3 ARN format as well as S3 URLs.
