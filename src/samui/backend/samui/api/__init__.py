from runners.helpers import db

from flask import Blueprint, request, jsonify
import logbook

logger = logbook.Logger(__name__)

rules_api = Blueprint('rules', __name__)


@rules_api.route('', methods=['GET'])
def get_rules():
    rule_type = request.args.get('type', '%').upper()
    rule_target = request.args.get('target', '%').upper()

    logger.info(f'Fetching {rule_target}_{rule_type} rules...')
    ctx = db.connect()
    result = ctx.cursor().execute(f"SHOW OBJECTS LIKE '%_{rule_target}\_{rule_type}' IN SCHEMA snowalert.rules;")
    NAME_COL = next(i for i, e in enumerate(result.description) if e[0] == 'name')
    ruleTitles = [row[NAME_COL] for row in result.fetchall()]
    rules = [{
        "title": title,
        "target": title.split('_')[-2].lower(),
        "type": title.split('_')[-1].lower(),
    } for title in ruleTitles if (
        title.endswith("_ALERT_QUERY")
        or title.endswith("_ALERT_SUPPRESSION")
        or title.endswith("_VIOLATION_QUERY")
        or title.endswith("_VIOLATION_SUPPRESSION")
    )]

    return jsonify(rules=rules)


@rules_api.route('', methods=['POST'])
def create_rule():
    logger.info(f'Creating rule...')
    json = request.get_json()
    rule_name, rule_type, rule_target, rule_body = json['name'], json['type'], json['target'], json['body']

    ctx = db.connect()
    ctx.cursor().execute(
        f"""CREATE OR REPLACE VIEW snowalert.rules.{rule_name}_{rule_target}_{rule_type} AS\n  {rule_body}"""
    ).fetchall()

    return jsonify(success=True)
