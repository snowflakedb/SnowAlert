import re
import os

import snowflake.connector

from runners.config import RULES_SCHEMA
from runners.helpers import db

from flask import Blueprint, request, jsonify
import logbook

logger = logbook.Logger(__name__)

rules_api = Blueprint('rules', __name__)

RULE_PREFIX = "^create( or replace)? view( if not exists)? [^ ]+ (copy grants )?as\n"
SECRET = os.environ.get("SECRET", "")


def unindent(text):
    "if beginning and every newline followed by spaces, removes them"
    indent = min(len(x) for x in re.findall('^( *)', text, flags=re.M | re.I))
    return text if indent == 0 else re.sub(f'^{" " * indent}', '', text, flags=re.M)


@rules_api.route('', methods=['GET'])
def get_rules():
    if request.cookies.get("sid", "") != SECRET:
        return jsonify(rules=[])

    rule_type = request.args.get('type', '%').upper()
    rule_target = request.args.get('target', '%').upper()

    logger.info(f'Fetching {rule_target}_{rule_type} rules...')
    ctx = db.connect()
    rules = db.fetch(ctx, f"SHOW VIEWS LIKE '%_{rule_target}\_{rule_type}' IN {RULES_SCHEMA};")
    return jsonify(
        rules=[
            {
                "title": re.sub('_(alert|violation)_(query|suppression)$', '', rule['name'], flags=re.I),
                "target": rule['name'].split('_')[-2].upper(),
                "type": rule['name'].split('_')[-1].upper(),
                "body": unindent(re.sub(RULE_PREFIX, '', rule['text'], flags=re.I)),
            } for rule in rules if (
                rule['name'].endswith("_ALERT_QUERY")
                or rule['name'].endswith("_ALERT_SUPPRESSION")
                or rule['name'].endswith("_VIOLATION_QUERY")
                or rule['name'].endswith("_VIOLATION_SUPPRESSION")
            )
        ]
    )


@rules_api.route('', methods=['POST'])
def create_rule():
    if request.cookies.get("sid", "") != SECRET:
        return jsonify(success=False, message="bad sid", rule={})

    json = request.get_json()
    rule_title, rule_type, rule_target, rule_body = json['title'], json['type'], json['target'], json['body']
    logger.info(f'Creating rule {rule_title}_{rule_target}_{rule_type}')

    ctx = db.connect()
    try:
        view_name = f"{rule_title}_{rule_target}_{rule_type}"
        ctx.cursor().execute(
            f"""CREATE OR REPLACE VIEW {RULES_SCHEMA}.{view_name} COPY GRANTS AS\n{rule_body}"""
        ).fetchall()
        if 'body' in json and 'savedBody' in json:
            json['savedBody'] = rule_body
    except snowflake.connector.errors.ProgrammingError as e:
        return jsonify(success=False, message=e.msg, rule=json)

    return jsonify(success=True, rule=json)


@rules_api.route('/delete', methods=['POST'])
def delete_rule():
    if request.cookies.get("sid", "") != SECRET:
        return jsonify(success=False, message="bad sid", rule={})

    json = request.get_json()
    rule_title, rule_type, rule_target, rule_body = json['title'], json['type'], json['target'], json['body']
    logger.info(f'Deleting rule {rule_title}_{rule_target}_{rule_type}')

    ctx = db.connect()
    try:
        view_name = f"{rule_title}_{rule_target}_{rule_type}"
        new_view_name = f"{rule_title}_{rule_target}_{rule_type}_DELETED"
        ctx.cursor().execute(
            f"""ALTER VIEW {RULES_SCHEMA}.{view_name} RENAME TO {RULES_SCHEMA}.{new_view_name}"""
        ).fetchall()
        if 'body' in json and 'savedBody' in json:
            json['savedBody'] = rule_body
    except snowflake.connector.errors.ProgrammingError as e:
        return jsonify(success=False, message=e.msg, rule=json)

    return jsonify(success=True, rule=json)


@rules_api.route('/rename', methods=['POST'])
def rename_rule():
    if request.cookies.get("sid", "") != SECRET:
        return jsonify(success=False, message="bad sid", rule={})

    json = request.get_json()
    rule_title, rule_type, rule_target, new_title = json['title'], json['type'], json['target'], json['newTitle']
    logger.info(f'Renaming rule {rule_title}_{rule_target}_{rule_type}')

    ctx = db.connect()
    try:
        view_name = f"{rule_title}_{rule_target}_{rule_type}"
        new_view_name = f"{new_title}_{rule_target}_{rule_type}"
        ctx.cursor().execute(
            f"""ALTER VIEW {RULES_SCHEMA}.{view_name} RENAME TO {RULES_SCHEMA}.{new_view_name}"""
        ).fetchall()
    except snowflake.connector.errors.ProgrammingError as e:
        return jsonify(success=False, message=e.msg, rule=json)

    return jsonify(success=True, rule=json)
