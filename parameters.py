from __future__ import annotations

"""
A script to list, read, and write aws SSM parameter store
"""

import os
import re
import json
import click
import boto3
import botocore.client
import dotenv
from tabulate import tabulate
from datetime import datetime
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Generator

diff_summary = namedtuple("diff_summary", "unchanged new changed")
change = namedtuple("change", "old new")


def is_valid_ssm_name(name: str) -> tuple[bool, str]:
    allowed = "[a-zA-Z0-9_.-]"
    arn_path = f"(?P<arn>arn:aws:ssm:\\w+-\\w+-\\d+:\\d+:parameter)?(?P<path>.+)"
    if arn_path_match := re.match(arn_path, name):
        arn, path = arn_path_match.groups()

    if len(name) > 1011:
        return False, "name cannot be longer than 1011 chars"
    if name.count("/") > 15:
        return False, "name cannot have more than 15 levels of hierachy"
    if re.match("(?i)/?(aws)|(ssm)", path):
        return False, "name cannot start with 'aws' or 'ssm'"
    if not re.match(f"^(/|{allowed})+$", path):
        return False, "name uses an illegal character or pattern"
    if not path.startswith("/") and "/" in path:
        return False, "name must be fully qualified path"
    return True, "name is valid"


def get_client(profile_name: str, resource_name: str) -> botocore.client.S3:
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

    return results


def put_parameter(client, name: str, value: str, param_type: str,
                  description: str | None = None, *, overwrite=False) -> str:
    if param_type not in ('String', 'StringList', 'SecureString'):
        raise ValueError(f"param type = {param_type} not accepted")

    client.put_parameter(
        Name=name,
        Description=description or "",
        Value=value,
        Type=param_type,
        Overwrite=overwrite,
    )
    return name


def put_parameters(profile_name: str, parameters: dict[str: str]) -> int:
    client = get_client(profile_name, "ssm")
    n_applied = 0
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(put_parameter, client, name, value, "SecureString", overwrite=True)
                   for name, value in parameters.items()]

        for future in as_completed(futures):
            if exc := future.exception():
                raise exc
            else:
                n_applied += 1
                click.echo(f"successfully put parameter {future.result()}", err=True)

    return n_applied


def walk_parameters(profile_name: str,
                    path: tuple[str | None, ...] | None = None,
                    limit: int | None=None,
                    *,
                    get_value: bool =False):
    """Generate parameters from parameter store, optionally filtered by a path

        The path can be full or partial, eg: '/path/to' or '/path/to/value'
    """
    client = get_client(profile_name, "ssm")
    path = path if path else (None, )

    def walk():
        for _path in path:
            if _path:
                full_path_filter = [{"Key": "Name", "Values": [_path]}]
                partial_path_filter = [{"Key": "Path", "Values": [_path]}]
                filters = (partial_path_filter, full_path_filter)
            else:
                filters = ([],)

            for param_filter in filters:
                page_iterator = (
                    client
                    .get_paginator("describe_parameters")
                    .paginate(ParameterFilters=param_filter, PaginationConfig={"MaxItems": limit})
                )

                for page in page_iterator:
                    parameters = page["Parameters"]
                    if get_value:
                        results = get_parameters(profile_name, (param["Name"] for param in parameters))

                        for param in parameters:
                            param["Value"] = results[param["Name"]]
                            yield param
                    else:
                        for param in parameters:
                            yield param

    yielded_param_names = set()

    for param in walk():
        if param["Name"] not in yielded_param_names:
            yield param
            yielded_param_names.add(param["Name"])


def diff_params(local_params: dict[str, str], remote_params: dict[str, str]) -> diff_summary:
    unchanged = {}
    changed = {}
    new = {}
    for local_key, local_value in local_params.items():
        if local_key in remote_params:
            if local_value == remote_params[local_key]:
                unchanged[local_key] = local_value
            else:
                changed[local_key] = change(old=remote_params[local_key], new=local_value)
        else:
            new[local_key] = local_value

    click.echo()

    if unchanged:
        click.secho("unchanged:", bold=True)
        rows = []
        for key, val in list(unchanged.items())[:3]:
            rows.append((click.style(f"\t\"{key}\":", dim=True),
                         click.style(f"\"{val}\"", dim=True)))
        click.echo(tabulate(rows, tablefmt="plain"))

        if len(unchanged) > 3:
            click.secho(f"\t... {len(unchanged) - 3} more unchanged", dim=True)
        click.echo()

    if new:
        click.secho("to add:", bold=True)
        rows = []
        for key, val in new.items():
            rows.append((click.style(f"\t\"{key}\":", fg='white'),
                         f"\"{click.style(val, fg='green')}\""))
        click.echo(tabulate(rows, tablefmt="plain"))
        click.echo()

    if changed:
        click.secho("to change:", bold=True)
        rows = []
        for key, _change in changed.items():
            rows.append((click.style(f"\t\"{key}\":", fg='white'),
                        f"\"{click.style(_change.old, fg='yellow')}\" -> \"{click.style(_change.new, fg='green')}\""))
        click.echo(tabulate(rows, tablefmt="plain"))
        click.echo()

    click.echo(f"{len(unchanged)} unchanged, "
               f"{click.style(len(new), fg='green')} new, "
               f"{click.style(len(changed), fg='yellow')} changed\n")

    return diff_summary(unchanged=unchanged, new=new, changed=changed)


@click.group()
@click.option("--profile", type=str)
@click.pass_context
def cli(ctx, **kwargs):
    for kw, val in kwargs.items():
        ctx.obj[kw] = val


@cli.command()
@click.argument("NAMES", type=str, nargs=-1)
@click.option("--path", default="", help="path to prepend to name(s)")
def check_name(names, path):
    """Check if a name is a valid SSM parameter name"""
    any_invalid = False
    for name in names:
        name = os.path.join(path, name)
        is_valid, message = is_valid_ssm_name(name)
        if is_valid:
            click.echo(f"{name!r} is valid {click.style('✓', fg='green', bold=True)}")
        else:
            click.echo(f"{name!r} is not valid: {message} {click.style('x', fg='red', bold=True)}")
            any_invalid = True

    if any_invalid:
        raise click.ClickException("one or more names were invalid")


@cli.command()
@click.option("--path", type=str, multiple=True, help="SSM parameter path (or part of)")
@click.option("-n", "--lines", type=int)
@click.option("--sort", type=click.Choice(("name", "modified", ""), case_sensitive=False), default="")
@click.option("--pager/--no-pager", default=True)
@click.option("--format", type=click.Choice(("table", "json", "names")), default="table")
@click.pass_context
def ls(ctx, path, lines, sort, pager, format):
    """List parameters"""

    parameters = walk_parameters(ctx.obj["profile"], path, lines)

    rows = ({"name": param["Name"],
             "type": param["Type"],
             "description": param.get("Description", ""),
             "modified-at": param["LastModifiedDate"]}
            for param in parameters)

    if sort:
        rows = sorted(rows, key=lambda line: line[{"name": "name", "modified": "modified at"}[sort]])

    if format == "table":
        str_to_print = tabulate(rows, headers="keys", tablefmt="orgtbl")
    elif format == "json":
        str_to_print = json.dumps(list(rows), indent=2, default=str)
    elif format == "names":
        str_to_print = "\n".join(row["name"] for row in rows)

    echo_fcn = click.echo_via_pager if pager else click.echo
    echo_fcn(str_to_print)


@cli.command()
@click.argument("env_file", type=click.File("w"), default="-")
@click.option("--path", multiple=True, type=str, help="SSM parameter path (or part of)")
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
@click.option("--dry-run", is_flag=True, default=False)
@click.pass_context
def push(ctx, env_file, path, dry_run):
    """Push parameters from a .env file to SSM

       All values will be stored as SecureString
    """
    local_params = {}
    for name, value in dotenv.dotenv_values(stream=env_file).items():
        param_name = os.path.join(path, name.strip().lower())

        is_valid, message = is_valid_ssm_name(param_name)
        if not is_valid:
            raise ValueError(f"parameter {param_name!r} is not valid. {message}")

        local_params[param_name] = value.strip()

    remote_params = {
        param["Name"]: param["Value"]["Value"] for param in
        walk_parameters(ctx.obj["profile"], path=tuple(local_params.keys()), get_value=True)
    }

    diff = diff_params(local_params, remote_params)

    if dry_run:
        click.echo("No changes made")
        return

    if diff.new or diff.changed:
        click.echo()
        answer = click.prompt("Are you sure you want to perform these actions?\n"
                              "Only 'yes' will accepted.\n\n"
                              f"{click.style('Enter a value', bold=True)}")
        if answer == 'yes':
            click.echo("applying...")
            params_to_apply = {**diff.new, **{name: chg.new for name, chg in diff.changed.items()}}
            n_applied = put_parameters(ctx.obj["profile"], params_to_apply)
            click.echo(f"Done! put {n_applied} parameters")

        else:
            click.echo("cancelling...")

    else:
        click.echo("Nothing to do")


def main():
    return cli(obj={})


if __name__ == "__main__":
    main()
