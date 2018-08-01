import argparse
import json

import apache_beam as beam
from apache_beam.io import BigQuerySource
from apache_beam.options.pipeline_options import PipelineOptions


class PrintRows(beam.DoFn):
    def __init__(self):
        super(PrintRows, self).__init__()

    def process(self, elems):
        (key, elem) = elems
        print("{}: {}".format(key, elem))
        return elems


class OutputToFile(beam.DoFn):
    def __init__(self, output):
        super(OutputToFile, self).__init__()
        self.output = output

    def process(self, author_json_pair):
        (author, data) = author_json_pair
        with open("{}/{}.json".format(self.output, author), "w") as f:
            f.write(data)


def row_to_json(author_row_pair):
    (author, row) = author_row_pair
    data = json.dumps(list(row))
    return (author, data)


def run(argv=None):
    known_args, options = pipeline_options(argv)
    with beam.Pipeline(options=options) as p:
        lines = p | "Read BigQuery" >> beam.io.Read(BigQuerySource(
            query="SELECT * FROM `bigquery-public-data.hacker_news.comments` LIMIT 100",
            use_standard_sql=True,
        ))
        with_author = (
            lines |
            "Add Author" >> beam.Map(lambda row: (row["author"], row)) |
            "Group By Autho" >> beam.GroupByKey()
        )
        author_jsons = with_author | "Convert Row To JSON" >> beam.Map(row_to_json)
        author_jsons | beam.ParDo(OutputToFile(known_args.output))


def pipeline_options(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        default="output",
        help="output directory",
    )
    known_args, pipeline_args = parser.parse_known_args(argv)
    return (known_args, PipelineOptions(pipeline_args))


if __name__ == "__main__":
    run()
