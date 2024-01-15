#! /usr/bin/env python
from __future__ import annotations

"""
A script to list and read aws cloudwatch logs
"""

import click
import boto3
from tabulate import tabulate
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


def get_client(profile_name, resource_name):
    session = boto3.Session(profile_name=profile_name)
    return session.client(resource_name)


def epoch_to_str(unix_epoch: int, fmt: str | None = None) -> str:
    return datetime.fromtimestamp(unix_epoch).strftime(fmt or "%Y-%m-%d %H:%M:%S %z")


def get_parameter(client, parameter_name):
    return client.get_parameter(Name=parameter_name, WithDecryption=True)["Parameter"]


def get_parameters(profile_name, parameter_names):
    results = {}
    client = get_client(profile_name, "ssm")
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(get_parameter, client, name): name for name in parameter_names}
        for future in as_completed(futures):
            if exc := future.exception():
                raise exc
            results[futures[future]] = future.result()
            click.echo(f"got result for {futures[future]}...", err=True)

    return results


def walk_parameters(profile_name, path=None, limit=None, *, get_value=False):
    """Generate parameters from parameter store, optionally filtered by a path"""
    filters = [{"Key": "Path", "Values": [path]}] if path else []

    client = get_client(profile_name, "ssm")

    page_iterator = (
        client
        .get_paginator("describe_parameters")
        .paginate(ParameterFilters=filters, PaginationConfig={"MaxItems": limit})
    )

    for page in page_iterator:
        parameters = page["Parameters"]
        if get_value:
            results = get_parameters(profile_name, (param["Name"] for param in parameters))

            for param in parameters:
                param["Value"] = results[param["Name"]]
                yield param
        else:
            yield from parameters


@click.group()
@click.option("--profile", type=str)
@click.pass_context
def cli(ctx, **kwargs):
    for kw, val in kwargs.items():
        ctx.obj[kw] = val


@cli.command()
@click.option("--path", type=str, help="SSM parameter path (or part of)")
@click.option("-n", "--lines", type=int)
@click.option("--sort", type=click.Choice(("name", "modified", ""), case_sensitive=False), default="")
@click.pass_context
def ls(ctx, path, lines, sort):
    """List parameters"""
    parameters = walk_parameters(ctx.obj["profile"], path, lines)

    rows = ({"name": param["Name"],
             "type": param["Type"],
             "description": param.get("Description", ""),
             "modified at": param["LastModifiedDate"]}
            for param in parameters)

    if sort:
        rows = sorted(rows, key=lambda line: line[{"name": "name", "modified": "modified at"}[sort]])

    click.echo(tabulate(rows, headers="keys", tablefmt="orgtbl"))


@cli.command()
@click.argument("env_file", type=click.File("w"), default="-")
@click.option("--path", type=str, help="SSM parameter path (or part of)")
@click.option("--decrypt/--no-decrypt", default=True)
@click.pass_context
def pull(ctx, env_file, path, decrypt):
    """Pull environment variables from SSM, output in .env format"""
    for param in walk_parameters(ctx.obj["profile"], path, get_value=decrypt):
        name = param["Name"].split("/")[-1].upper()
        value = param.get("Value", {}).get("Value", "")
        env_file.write(f"{name}={value}\n")


@cli.command()
@click.argument("env_file", type=click.File("r"), default="-")
@click.option("--path", type=str, help="SSM parameter path (or part of)")
@click.pass_context
def push(ctx, env_file, path):
    """Push parameters from a .env file to SSM"""
    local_params = {}

    while line := env_file.readline():
        if "=" not in line:
            raise ValueError(f"badly formatted .env file. expected '=' but not found on {line=}")
        name, _, value = line.partition("=")
        local_params[name.strip().lower()] = value.strip()

    remote_params = {
        param["Name"]: param for param in
        walk_parameters(ctx.obj["profile"], path=path, get_value=True)
    }

    for key in set(remote_params) | set(local_params):
        click.echo(f"{key} {local_params.get(key)} {remote_params.get(key)}")


def main():
    return cli(obj={})


if __name__ == "__main__":
    main()
