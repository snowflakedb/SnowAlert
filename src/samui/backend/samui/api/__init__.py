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
    result = ctx.cursor().execute(f"SHOW VIEWS LIKE '%_{rule_target}\_{rule_type}' IN snowalert.rules;")
    COLS = [col[0] for col in result.description]
    rules = [dict(zip(COLS, row)) for row in result.fetchall()]
    return jsonify(
        rules=[
            {
                "title": rule['name'],
                "target": rule['name'].split('_')[-2].lower(),
                "type": rule['name'].split('_')[-1].lower(),
                "body": rule['text'],
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
    logger.info(f'Creating rule...')
    json = request.get_json()
    rule_name, rule_type, rule_target, rule_body = json['name'], json['type'], json['target'], json['body']

    ctx = db.connect()
    ctx.cursor().execute(
        f"""CREATE OR REPLACE VIEW snowalert.rules.{rule_name}_{rule_target}_{rule_type} AS\n  {rule_body}"""
    ).fetchall()

    return jsonify(success=True)
