import re

from runners.helpers import db

from flask import Blueprint, request, jsonify
import logbook

logger = logbook.Logger(__name__)

rules_api = Blueprint('rules', __name__)

RULE_PREFIX = "^create( or replace)? view( if not exists)? [^ ]+ as\n"


def unindent(text):
    "if beginning and every newline followed by spaces, removes them"
    indent = min(len(x) for x in re.findall('^( *)', text, flags=re.M | re.I))
    return text if indent == 0 else re.sub(f'^{" " * indent}', '', text, flags=re.M)


@rules_api.route('', methods=['GET'])
def get_rules():
    rule_type = request.args.get('type', '%').upper()
    rule_target = request.args.get('target', '%').upper()

    logger.info(f'Fetching {rule_target}_{rule_type} rules...')
    ctx = db.connect()
    result = ctx.cursor().execute(f"SHOW VIEWS LIKE '%_{rule_target}\_{rule_type}' IN snowalert.rules;")
    cols = [col[0] for col in result.description]
    rules = [dict(zip(cols, row)) for row in result.fetchall()]
    return jsonify(
        rules=[
            {
                "title": re.sub('_(alert|violation)_(query|suppression)$', '', rule['name'], flags=re.I),
                "target": rule['name'].split('_')[-2].lower(),
                "type": rule['name'].split('_')[-1].lower(),
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
    json = request.get_json()
    rule_title, rule_type, rule_target, rule_body = json['title'], json['type'], json['target'], json['body']
    logger.info(f'Creating rule {rule_title}_{rule_target}_{rule_type}')

    ctx = db.connect()
    ctx.cursor().execute(
        f"""CREATE OR REPLACE VIEW snowalert.rules.{rule_title}_{rule_target}_{rule_type} AS\n  {rule_body}"""
    ).fetchall()

    return jsonify(success=True, rule=json)
