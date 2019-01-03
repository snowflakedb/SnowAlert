import {Avatar, Badge, Button, Card, List, Table, Row} from 'antd';
import React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';
import '../../index.css';

import {getRules} from '../../reducers/rules';
import * as stateTypes from '../../reducers/types';
import {
  changeTitle,
  changeRule,
  newRule,
  renameRule,
  updateInterimTitle,
  loadSnowAlertRules,
} from '../../actions/rules';
import './Policies.css';

interface StateProps {
  rules: stateTypes.SnowAlertRulesState;
}

interface DispatchProps {
  loadSnowAlertRules: typeof loadSnowAlertRules;
  newRule: typeof newRule;
  changeRule: typeof changeRule;
  changeTitle: typeof changeTitle;
  renameRule: typeof renameRule;
  updateInterimTitle: typeof updateInterimTitle;
}

type PoliciesProps = StateProps & DispatchProps;

function raise(e: string): never {
  throw e;
}

// function matchAll(regexp: RegExp, s: string): string[][] {
//   var matches: string[][] = [];
//   s.replace(regexp, (...args) => {
//     matches.push(args);
//     return '';
//   });
//   return matches;
// };

function successDot(status: boolean) {
  return status ? (
    <Avatar size={15} style={{color: 'green', backgroundColor: '#b5e2a2'}} />
  ) : (
    <Avatar size={15} style={{color: 'red', backgroundColor: '#fde3cf'}} />
  );
}

class PolicyRule {
  _raw: stateTypes.SnowAlertRule;
  views: string;
  comment: string;
  subpolicies: {title: string; passing: boolean; condition: string}[];

  constructor(rule: stateTypes.SnowAlertRule) {
    this.raw = rule;
  }

  get raw() {
    return this._raw;
  }

  set raw(r) {
    this._raw = r;
    this.parse_body(r.body, r.results);
  }

  get view_name(): string {
    return this.raw.title + '_POLICY_DEFINITION';
  }

  get passing(): boolean {
    return this.subpolicies.every(x => x.passing);
  }

  get title() {
    return this.comment.replace(/\n.*$/g, '');
  }

  get description() {
    return this.comment.replace(/^.*?\n/g, '');
  }

  parse_body(sql: string, results: stateTypes.SnowAlertRule['results']) {
    const vnameRe = /^CREATE OR REPLACE VIEW [^.]+.[^.]+.([^\s]+) COPY GRANTS\n  /m,
      descrRe = /^COMMENT='([^']+)'\nAS\n/gm,
      subplRe = /^  SELECT '([^']+)' AS title\n       , ([^;]+?) AS passing$(?:\n;|\nUNION ALL\n)?/m;

    const vnameMatch = vnameRe.exec(sql) || raise('no vname match'),
      vnameAfter = sql.substr(vnameMatch[0].length);

    const descrMatch = descrRe.exec(vnameAfter) || raise('no descr match'),
      descrAfter = vnameAfter.substr(descrMatch[0].length);

    this.comment = descrMatch[1];
    this.subpolicies = [];

    var rest = descrAfter;
    var i = 0;

    do {
      var matchSubpl = subplRe.exec(rest) || raise('no title match'),
        rest = rest.substr(matchSubpl[0].length);

      this.subpolicies.push({
        passing: results ? results[i++].PASSING : false,
        title: matchSubpl[1],
        condition: matchSubpl[2],
      });
    } while (rest);
  }

  get body(): string {
    return (
      `CREATE OR REPLACE VIEW snowalert.rules.${this.view_name}_POLICY_DEFINITION COPY GRANTS\n` +
      `  COMMENT='${this.title}\n'` +
      `AS\n` +
      this.subpolicies
        .map(sp => `  SELECT '${sp.title}' AS title\n` + `       , '${sp.condition}' AS passing`)
        .join('\nUNION ALL\n') +
      `;\n`
    );
  }
}

class Policies extends React.PureComponent<PoliciesProps> {
  componentDidMount() {
    this.props.loadSnowAlertRules();
    this.props.changeRule('');
  }

  render() {
    const {
      rules: {rules, currentRuleView},
    } = this.props;

    const policies = rules.filter(r => r.target == 'POLICY').map(r => new PolicyRule(r));

    return (
      <Card
        title={'Policies Dashboard'}
        className={'card'}
        bordered={true}
        extra={
          <div>
            <Button type="primary" onClick={() => this.props.newRule('POLICY', 'DEFINITION')}>
              + POLICY
            </Button>
          </div>
        }
      >
        <Row>
          <List
            itemLayout="vertical"
            dataSource={policies}
            pagination={false}
            renderItem={(policy: PolicyRule) => (
              <List.Item>
                <List.Item.Meta
                  title={
                    <span>
                      <Badge
                        count={`${policy.subpolicies.filter(x => x.passing).length}`}
                        style={{color: 'green', backgroundColor: '#b5e2a2', marginRight: 10}}
                      />
                      <Badge
                        count={`${policy.subpolicies.filter(x => !x.passing).length}`}
                        style={{color: 'red', backgroundColor: '#fde3cf', marginRight: 10}}
                      />
                      <a
                        onClick={() =>
                          this.props.changeRule(policy.view_name == currentRuleView ? '' : policy.view_name)
                        }
                      >
                        {policy.title}
                      </a>
                    </span>
                  }
                  description={policy.description}
                />
                <div>
                  {policy.view_name == currentRuleView && (
                    <Table
                      pagination={false}
                      columns={[
                        {title: '', dataIndex: 'passing', key: 'passing', render: successDot, width: 5},
                        {title: 'title', dataIndex: 'title', key: 'title'},
                        {title: 'condition', dataIndex: 'condition', key: 'condition'},
                      ]}
                      dataSource={policy.subpolicies}
                    />
                  )}
                </div>
              </List.Item>
            )}
          />
        </Row>
      </Card>
    );
  }
}

const mapStateToProps = (state: stateTypes.State) => {
  return {
    rules: getRules(state),
  };
};

const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators(
    {
      loadSnowAlertRules,
      newRule,
      changeRule,
      changeTitle,
      renameRule,
      updateInterimTitle,
    },
    dispatch,
  );
};

export default connect<StateProps, DispatchProps>(
  mapStateToProps,
  mapDispatchToProps,
)(Policies);
