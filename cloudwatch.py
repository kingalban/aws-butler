#! /usr/bin/env python
from __future__ import annotations

"""
A script to list and read aws cloudwatch logs
"""

import re
import json
import click
import boto3
from tabulate import tabulate
from datetime import datetime, timedelta

ANSI_COLOR = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')


def no_color(string):
    return ANSI_COLOR.sub("", string)


def epoch_to_str(unix_epoch: int, fmt: str | None = None) -> str:
    return datetime.fromtimestamp(unix_epoch).strftime(fmt or "%Y-%m-%d %H:%M:%S")


def interval_to_str(n_seconds: int) -> str:
    return str(timedelta(seconds=n_seconds)).partition(".")[0]


def unix_epoch_24_hrs_ago():
    current_datetime = datetime.now()
    datetime_24_hours_ago = current_datetime - timedelta(hours=24)
    return int(datetime_24_hours_ago.timestamp()) * 1000


@click.group()
@click.option("--profile", required=True, type=str)
@click.option("--log-group-name", required=True, type=str)
@click.pass_context
def cli(ctx, profile, log_group_name):
    ctx.obj["log-group-name"] = log_group_name
    ctx.obj["session"] = boto3.Session(profile_name=profile)


def walk_log_streams(client, log_group_name, limit=None, jmespath_filter=None):
    page_iterator = (
        client
        .get_paginator("describe_log_streams")
        .paginate(logGroupName=log_group_name,
                  orderBy="LastEventTime",
                  descending=True,
                  PaginationConfig={"MaxItems": limit, "PageSize": 10})
    )

    if jmespath_filter:
        page_iterator = page_iterator.search(jmespath_filter)

    for page in page_iterator:
        if jmespath_filter:
            yield page
        else:
            yield from page['logStreams']


@cli.command()
@click.option("-n", "--lines", default=20)
@click.option("--today", is_flag=True, default=False, help="Limit results to the last 24 hours")
@click.option("--format", type=click.Choice(("table", "json", "lines"), case_sensitive=False), default="table")
@click.pass_context
def ls(ctx, lines, today, format):
    """List log streams in the log group, ordered by last event time descending"""
    client = ctx.obj["session"].client("logs")
    log_group_name = ctx.obj["log-group-name"]
    jmespath_filter = f"logStreams[?creationTime >= `{unix_epoch_24_hrs_ago()}`][]" if today else None

    log_streams = walk_log_streams(client, log_group_name, lines, jmespath_filter)

    if format == "table":
        rows = ({"name": stream["logStreamName"],
                 "created at": epoch_to_str(stream["creationTime"] / 1000),
                 "latest event at": epoch_to_str(stream["lastEventTimestamp"] / 1000),
                 "duration": interval_to_str(
                     stream["lastEventTimestamp"] / 1000 - stream["firstEventTimestamp"] / 1000)}
                for stream in log_streams)

        click.echo(tabulate(rows, headers="keys", tablefmt="orgtbl"))

    elif format == "lines":
        for stream in log_streams:
            click.echo(stream['logStreamName'])

    elif format == "json":
        click.echo(json.dumps([*log_streams]))


def walk_log_events(client, log_group_name, log_stream_name, *,
                    limit=None, page_size=None, start_from_head=False, unmask=False):
    next_token = None
    n_events = 0
    _limit = page_size or limit or 10_000
    kwargs = {"logGroupName": log_group_name,
              "logStreamName": log_stream_name,
              "limit": _limit if _limit < 10_000 else 10_000,
              "startFromHead": start_from_head,
              "unmask": unmask}
    while True:
        response = client.get_log_events(**kwargs, **({"nextToken": next_token} if next_token else {}))
        yield from response["events"]
        n_events += len(response["events"])
        if next_token == response.get("nextForwardToken") or (limit and n_events >= limit):
            return
        next_token = response.get("nextForwardToken")


def print_log_stream(session, log_stream_names, log_group_name, use_pager, use_color, **kwargs):
    def walk_lines(_client, _log_stream_names, _log_group_name, **_kwargs):
        if not _log_stream_names:
            _log_stream_names = (stream["logStreamName"] for stream in walk_log_streams(_client, _log_group_name))

        for log_stream_name in _log_stream_names:
            events = walk_log_events(_client, _log_group_name, log_stream_name, **_kwargs)

            formatted_events = (f"{epoch_to_str(event['timestamp'] / 1000)}: {event['message']}\n" for event in events)

            if not use_color:
                click.echo("removing color...", err=True)
                formatted_events = (no_color(line) for line in formatted_events)

            yield f"{_log_group_name} {log_stream_name}\n"
            yield from formatted_events
            yield "\n"

    client = session.client("logs")

    lines_to_print = walk_lines(client, log_stream_names, log_group_name, **kwargs)

    if use_pager:
        click.echo_via_pager(lines_to_print)
    else:
        click.echo("".join(lines_to_print))


@cli.command()
@click.argument("log_stream_names", type=str, default=None, nargs=-1)
@click.option("--pager/--no-pager", default=True)
@click.option("--color/--no-color", default=True)
@click.pass_context
@click.option("--page-size")
def cat(ctx, log_stream_names, page_size, pager, color):
    """Print the content of a log stream"""
    print_log_stream(ctx.obj["session"], log_stream_names, ctx.obj["log-group-name"], pager,
                     page_size=page_size, start_from_head=True, use_color=color)


@cli.command()
@click.argument("log_stream_names", type=str, default=None, nargs=-1)
@click.option("--pager/--no-pager", default=True)
@click.option("--color/--no-color", default=True)
@click.pass_context
@click.option("-n", "--lines", default=10)
def head(ctx, log_stream_names, pager, lines, color):
    """Print the first n lines of a log stream"""
    print_log_stream(ctx.obj["session"], log_stream_names, ctx.obj["log-group-name"], pager,
                     limit=lines, start_from_head=True, use_color=color)


@cli.command()
@click.argument("log_stream_names", type=str, default=None, nargs=-1)
@click.option("--pager/--no-pager", default=True)
@click.option("--color/--no-color", default=True)
@click.pass_context
@click.option("-n", "--lines", default=10)
def tail(ctx, log_stream_names, pager, lines, color):
    """Print the last n lines of a log stream"""
    print_log_stream(ctx.obj["session"], log_stream_names, ctx.obj["log-group-name"], pager,
                     limit=lines, start_from_head=False, use_color=color)


def main():
    return cli(obj={})


if __name__ == "__main__":
    main()
