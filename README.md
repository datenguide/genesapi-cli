# genesapi-cli

A command-line interface to download, process and index german public statistic
data from *GENESIS*-Instances like
[www.regionalstatistik.de](http://www.regionalstatistik.de/) into a `JSON`
format that can then be loaded via
[Logstash](https://www.elastic.co/products/logstash) into an
[Elasticsearch](https://www.elastic.co/products/elasticsearch) index.

This package has a very modular approach so it can be used for different
purposes when dealing with german public statistics data from *GENESIS*.

For example, it is used within (and developed for) the
[datengui.de](https://datengui.de) tech stack to provide a
[GraphQL](https://graphql.org)-API that feeds the website.

## install

`genesapi-cli` requires python 3.

We recommend organizing your python stuff via pip and virtual environments.

**FIXME**: `genesapi-cli` relies on a fork of
[regenesis](https://github.com/pudo/regenesis), that needs to be installed
manually beforehand, any pull requests to hook this properly into our
`setup.py` are welcomed ;)

    pip install -e git+git@github.com:datenguide/regenesis.git#egg=regenesis

    pip install -e git+git@github.com:datenguide/genesapi-cli.git#egg=genesapi

This will install the `genesapi` command-line interface and all the
requirements, like [pandas](https://pandas.pydata.org) (see their [install
documentation](https://pandas.pydata.org/pandas-docs/stable/install.html) if
you run into trouble during installation).

*Note:* Although *the very very great* `regenesis` package is a bit outdated,
`genesapi` is only importing some parts of it, so don't worry if you don't get
`regenesis` running by yourself locally. But *if* you get it running, its
creator [@pudo](https://github.com/pudo/) + the
[datenguide-team](https://github.com/orgs/datenguide/people) would be very
happy about pull requests :-)

After installing, you should be able to type this into your command line:

    genesapi -h

## usage

*Wait:* If you ended up here because you just want to set up a small local
instance of the [genesapi flask app](https://github.com/datenguide/datenguide-backend)
that exposes the `GraphQL`-api, [follow the steps described
there](https://github.com/datenguide/datenguide-backend#setup-elasticsearch-locally-with-sample-data).

## tl;dr

A complete process to download the complete *GENESIS* data and upload it into
an Elasticsearch index is listed below – continue reading the [Full
Documentation](#full-documentation) if you need to understand whats going on exactly.

Before *really executing* this, read at least the notes below this list.

    mkdir ./data/{cubes,attributes}
    genesapi fetch catalog.yml ./data/cubes/
    genesapi fetch_attributes catalog.yml ./data/attributes/
    genesapi build_schema ./data/cubes/ --attributes ./data/attributes/ > schema.json
    genesapi build_es_template ./schema.json > template.json
    curl -H 'Content-Type: application/json' -XPOST http://localhost:9200/_template/genesapi -d@template.json
    genesapi jsonify cubes | logstash -f logstash.conf

Its roughly about to download 1.2G from the *GENESIS* soap api. The
Elasticsearch index well be around 25G at the end. The soap api is very slow
and the complete download (currently 2278 single csv files aka *cubes*) takes
*several* hours, as well as the indexing into Elasticsearch.

Plus, most of the scripts make use of python3's built-in
[multiprocessing](https://docs.python.org/3.5/library/multiprocessing.html), so
most of the time all the cores of the machine that is running these commands
are in heavy use.

It is as well highly recommended that you make yourself a bit familiar with
[Elasticsearch](https://www.elastic.co/products/elasticsearch) and
[Logstash](https://www.elastic.co/products/logstash) and how to tweak it for
your machine(s).

## Example data

For playing around with this package *without* downloading from the GENESIS
api, there is some downloaded data in the `example/` folder of this repo.

## Full Documentation

This package is split in several small tasks, that can all be invoked via

    genesapi <task> arg1 --kwarg1 foo

The tasks are all a step within the general "data pipeline" that downloads raw
csv data *cubes* and transform them into a json-serializable format.

### Tasks:

1. [**fetch**](#fetch)
2. [fetch_attributes](#fetch_attributes)
3. [build_schema](#build_schema)
4. [build_markdown](#build_markdown)
5. [build_es_template](#build_es_template)
6. [**jsonify**](#jsonify)

For transforming csv data *cubes* to json *facts*, only `fetch` and `jsonify`
are necessary.

To provide some context data & schema definitions to load the jsonified data
into Elasticsearch, the other tasks are needed in between (see below)

##### Logging

`genesapi` prints a lot to `stdout`, for instance the jsonified *facts* so that
they can easily piped to logstash, so logging happens to `stderr`

You can adjust the logging level (default: `INFO`) to any valid python logging
level, for example:

    genesapi --logging DEBUG <task> <args>

#### fetch

Download csv data (aka *cubes*) from a *GENESIS* instance like
[www.regionalstatistik.de](https://www.regionalstatistik.de) and store them
somewhere in the local filesystem.

Create a *catalog* in `YAML` format, see `example/catalog.yml` for details
(basically, just put in your credentials)

```
usage: genesapi fetch [-h] [--replace] catalog output

positional arguments:
  catalog     YAML file with regensis catalog config
  output      Directory where to store cube data

optional arguments:
  -h, --help  show this help message and exit
  --replace   Replace existing (previously downloaded) cubes
```

Example:

    genesapi fetch catalog.yml ./data/cubes/

#### jsonify

Transform downloaded *cubes* (csv files) into *facts* (json lines)

A fact is *a unique value of a specific topic at a specific location at a
specific point in time (or timespan)*

- value: a number, either `int` or `float`
- topic: a broader topic like "work statistics" described with a combination of
  *attributes* and *filters*, e.g. "Gender: Female, Age: 15 to 20 years"
- location: Germany itself or a state, district or municipality in Germany.
- time: either a year or a specific date.

The *number of flats* (`WOHNY1`) that have *5 rooms* (` WHGGR1` = `WHGRME05`)
in the german municipality *Baddeckenstedt* (`id`: 03158402) in the state of
"Niedersachsen" in *2016* (`year`) was **1120** (`value`).

In the current implementation, this described fact looks like this in json:

```json
{
    "year" : "2016",
    "WOHNY1" : {
        "error" : "0",
        "quality" : "e",
        "locked" : "",
        "value" : 1120
    },
    "STAG" : {
        "until" : "2016-12-31T23:59:59",
        "value" : "31.12.2016",
        "from" : "2016-12-31T00:00:00"
    },
    "fact_id" : "394ce1e5e76fdb9599c46ecbb3db6c8f8ae09c33",
    "id" : "03158402",
    "cube" : "31231GJ006",
    "GEMEIN" : "03158402",
    "WHGGR1" : "WHGRME05",
    "lau" : 1
}
```

[See this fact query at api.genesapi.org](https://api.genesapi.org/?query=%7B%0A%20%20region(id%3A%20%2203158402%22)%20%7B%0A%20%20%20%20id%0A%20%20%20%20name%0A%20%20%20%20WOHNY1(year%3A%20%222016%22%2C%20WHGGR1%3A%20%22WHGRME05%22)%20%7B%0A%20%20%20%20%20%20year%0A%20%20%20%20%20%20value%0A%20%20%20%20%20%20id%0A%20%20%20%20%20%20source%20%7B%0A%20%20%20%20%20%20%20%20name%0A%20%20%20%20%20%20%20%20valid_from%0A%20%20%20%20%20%20%20%20url%0A%20%20%20%20%20%20%20%20title_de%0A%20%20%20%20%20%20%20%20periodicity%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%7D%0A%20%20%7D%0A%7D%0A)

You can either store the facts as json files on disk or directly pipe them to
[logstash](https://www.elastic.co/products/logstash)

*Note:* Storing a whole json-serialized *GENESIS* dump to disk requires a lot
of time and space. The option to store the facts as json files is more for
debugging purposes or to share serialized subsets of the data accross devices
or people.  We recommend directly piping to logstash if you want to feed a
complete Elasticsearch index (which takes a lot of time and space, too...).

```
usage: genesapi jsonify [-h] [--output OUTPUT] [--pretty] directory

positional arguments:
  directory        Directory with raw cubes downloaded via the `fetch` command

optional arguments:
  -h, --help       show this help message and exit
  --output OUTPUT  Output directory. If none, print each record per line to
                   stdout
  --pretty         Print pretty indented json (for debugging purposes)
```

**How to use this command to feed an Elasticsearch index**

Download logstash and install it somehow, use the logstash config in this repo.

    genesapi jsonify cubes | logstash -f logstash.conf

[See here a more detailed description how to set up an Elasticsearch cluster
for genesapi](https://github.com/datenguide/datenguide-backend#setup-elasticsearch-locally-with-sample-data)

#### fetch_attributes

Download attributes (aka "Merkmale") info from the *GENESIS* api to provide
some more context for [`build_schema`](#build_schema) and
[`build_markdown`](#build_markdown)

Catalog: See [`fetch`](#fetch).

```
usage: genesapi fetch_attributes [-h] [--replace] catalog output

positional arguments:
  catalog     YAML file with catalog config
  output      Directory where to store attributes data

optional arguments:
  -h, --help  show this help message and exit
  --replace   Replace existing (previously downloaded) attributes
```

Example:

    genesapi fetch_attributes catalog.yml ./data/attributes/

#### build_schema

The schema is needed for the [flask
app](https://github.com/datenguide/datenguide-backend) and for the tasks
[`build_es_template`](#build_es_template) and
[`build_markdown`](#build_markdown).

This commands grabs the raw *cubes* and extracts the attribute ("Merkmal")
structure out of it into a json format printed to `stdout`.

Optionally using the attributes data (`--attributes`) from
[`fetch_attributes`](#fetch_attributes) provides the schema with longer
descriptions for the attributes, that can be rendered e.g. into a documentation
or used by [`build_markdown`](#build_markdown)

```
usage: genesapi build_schema [-h] [--attributes ATTRIBUTES] directory

positional arguments:
  directory             Directory with raw cubes downloaded via the `fetch`
                        command

optional arguments:
  -h, --help            show this help message and exit
  --attributes ATTRIBUTES
                        Directory where JSON files of attribute descriptions
                        are stored
```

Example including attributes data:

    genesapi build_schema ./data/cubes/ --attributes ./data/attributes/ > schema.json

#### build_es_template

Create a template mapping for Elasticsearch, based on the schema from
[`build_schema`](#build_schema)

```
usage: genesapi build_es_template [-h] [--index INDEX] [--shards SHARDS]
                                  [--replicas REPLICAS]
                                  schema

positional arguments:
  schema               JSON file from `build_schema` output

optional arguments:
  -h, --help           show this help message and exit
  --index INDEX        Name of elasticsearch index
  --shards SHARDS      Number of shards for elasticsearch index
  --replicas REPLICAS  Number of replicas for elasticsearch index
```

Example:

    genesapi build_es_template ./data/schema.json > template.json

Apply this template (index name *genesapi*, could be anything):

    curl -H 'Content-Type: application/json' -XPOST http://localhost:9200/_template/genesapi -d@template.json

[See here a more detailed description how to set up an Elasticsearch cluster
for genesapi](https://github.com/datenguide/datenguide-backend#setup-elasticsearch-locally-with-sample-data)

#### build_markdown

Export each *attribute* (from [`fetch_attributes`](#fetch_attributes), wrangled
into 1 json file via [`build_schema`](#build_schema)) to a markdown with
frontmatter that could be used to generate a documentation page powered by
[jekyll](https://jekyllrb.com/) or [gatsby](https://www.gatsbyjs.org/).

```
usage: genesapi build_markdown [-h] schema output

positional arguments:
  schema      JSON file from `build_schema` output
  output      Output directory.

optional arguments:
  -h, --help  show this help message and exit
```

Example:

    genesapi build_markdown ./data/schema.json ../path-to-my-jekyll/_posts/
