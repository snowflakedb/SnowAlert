import {Button, Checkbox, Col, Icon, Input} from 'antd';
import {isEqual} from 'lodash';
import * as React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';

import {getRules} from '../../reducers/rules';
import {changeRuleBody, saveRule} from '../../actions/rules';

import {State, SnowAlertRule, SnowAlertRulesState} from '../../reducers/types';

import './FormEditor.css';

interface QueryFields {
  select: {
    [prop: string]: string;
  };
  from: string;
  enabled: boolean;
  where: string;
}

interface SuppressionFields {
  from: string;
  rulesString: string;
  rules: Array<string>;
}

interface ParserGenerator<T> {
  parse: (body: string) => T | null;
  generate: (forms: T) => string;
}

function raise(e: string): never {
  throw e;
}

const suppressionSQL: ParserGenerator<SuppressionFields> = {
  parse: body => {
    function stripStart(body: string): {rest: string; from: string} | null {
      const headRe = /^SELECT \*\s+FROM ([a-z.]+)\s+WHERE suppressed IS NULL\s+/im;
      const m = body.match(headRe);
      return m ? {rest: body.substr(m[0].length), from: m[1]} : null;
    }

    function stripRule(body: string): {rule: string; rest: string} | null {
      const ruleRe = /^(\s*(;\s*|AND[\s\S]*?)\s*)(?:AND[\s\S]*|;)/im;
      const m = body.match(ruleRe);
      return m ? {rule: m[2], rest: body.substr(m[1].length)} : null;
    }

    var {rest, from} = stripStart(body) || raise('err0');
    const rulesString = rest.replace(/;\s*$/gm, ''); // hack until array UI ready
    var {rule, rest} = stripRule(rest) || raise('err1');
    var rules = [];
    while (rest.length > 1) {
      rules.push(rule);
      var {rule, rest} = stripRule(rest) || raise('err2');
    }
    rules.push(rule);
    return {rules, from, rulesString};
  },
  generate: fields => {
    // const whereClauseLines = ['WHERE 1=1', 'AND suppressed IS NULL'].concat(fields.rules)
    return (
      `SELECT *\n` + `FROM ${fields.from}\n` + `WHERE suppressed IS NULL\n` + `${fields.rulesString};`
      // `${whereClauseLines.join('\n  ')}\n;`
    );
  },
};

const querySQL: ParserGenerator<QueryFields> = {
  parse: body => {
    function stripField(body: string): {body: string; field: string; value: string} | null {
      const match = body.match(/^\s*(?:SELECT |,)\s*([\s\S]*?) AS ([\S^,]*)$/im);
      if (match) {
        const [m, value, field] = match;
        return {
          body: body.substr(m.length),
          field,
          value,
        };
      } else {
        return null;
      }
    }

    function stripFrom(body: string): {body: string; from: string} {
      const [match, from] = body.match(/^\s*FROM (\S*)/i) || raise('err1');
      return {
        body: body.substr(match.length),
        from,
      };
    }

    function stripWhere(body: string): {enabled: boolean; where: string; more: boolean} {
      const [match, enabled, where] = body.match(/^\s*WHERE 1=([0|1])\s*AND ([\s\S]*?)\s*;$/im) || raise('err2');
      return {
        enabled: enabled === '1',
        where,
        more: match.length !== body.length,
      };
    }

    var fields = {
      select: {},
      from: '',
      enabled: false,
      where: '',
    };

    var nextField = stripField(body);
    if (!nextField) throw 'err0';
    do {
      var {body, field, value} = nextField;
      fields.select[field] = value;
    } while ((nextField = stripField(body)));

    var {body, from} = stripFrom(body);
    fields.from = from;

    var {enabled, where} = stripWhere(body);
    fields.enabled = enabled;
    fields.where = where;

    return fields;
  },
  generate: fields => {
    return (
      `SELECT ${Object.entries(fields.select)
        .map(([k, v]) => `${v} AS ${k}`)
        .join('\n     , ')}\n` +
      `FROM ${fields.from}\n` +
      `WHERE 1=${fields.enabled ? '1' : '0'}\n` +
      `  AND ${fields.where}\n;`
    );
  },
};

function canParse(rule: SnowAlertRule | null, debug: boolean = false): boolean {
  if (!rule) return false;
  const SQL: any = rule.type === 'SUPPRESSION' ? suppressionSQL : querySQL;
  try {
    const fields = SQL.parse(rule.body);
    if (debug) {
      console.log('body', rule.body);
      console.log('parse', SQL.parse(rule.body));
      console.log('generate', SQL.generate(SQL.parse(rule.body)));
    }
    return Boolean(fields && isEqual(fields, SQL.parse(SQL.generate(fields))));
  } catch (e) {
    if (debug) {
      console.log(e);
    }
    return false;
  }
}

interface OwnProps {}

interface DispatchProps {
  changeRuleBody: typeof changeRuleBody;
  saveRule: typeof saveRule;
}

interface StateProps {
  rules: SnowAlertRulesState;
}

type FormEditorProps = OwnProps & DispatchProps & StateProps;

class FormEditor extends React.PureComponent<FormEditorProps> {
  render() {
    const {changeRuleBody} = this.props;
    const {currentRuleView, rules} = this.props.rules;
    const rule = rules.find(r => `${r.title}_${r.target}_${r.type}` == currentRuleView);
    if (!rule)
      return (
        <Col span={12}>
          <h3>No Rule Selected</h3>
        </Col>
      );

    const SQL = rule.type === 'SUPPRESSION' ? suppressionSQL : querySQL;
    const fields = canParse(rule) ? SQL.parse(rule.body) : null;
    if (!fields) {
      return (
        <Col span={12}>
          <h3>Can't parse</h3>
        </Col>
      );
    }

    function changeField(fieldName: string, prop: string = 'select') {
      return ({target}: any) => {
        if (!rule || !target) return;
        var fields = SQL.parse(rule.body);
        if (!fields) return;
        if (fieldName === '') {
          fields[prop] = target.value || target.checked;
        } else {
          fields[prop][fieldName] = target.value || target.checked;
        }
        changeRuleBody((SQL.generate as any)(fields));
      };
    }

    var form;
    if (rule.type === 'SUPPRESSION') {
      const fs = fields as SuppressionFields;
      form = (
        <div>
          <Input.TextArea
            disabled={!canParse(rule) || rule.isSaving}
            autosize={{minRows: 30, maxRows: 50}}
            spellCheck={false}
            value={fs.rulesString || ''}
            onChange={changeField('', 'rulesString')}
            style={{fontFamily: 'Hack, monospace'}}
          />
        </div>
      );
    } else {
      const fs = fields as QueryFields;
      if (rule.target === 'VIOLATION') {
        form = (
          <div>
            <Col span={12}>
              <h3>Environment</h3>
              <Input.TextArea
                disabled={!canParse(rule) || rule.isSaving}
                autosize={{minRows: 1}}
                value={fs ? fs.select.environment : ''}
                onChange={changeField('environment')}
              />

              <h3>Object</h3>
              <Input.TextArea
                disabled={!canParse(rule) || rule.isSaving}
                autosize={{minRows: 1}}
                value={fs ? fs.select.object : ''}
                onChange={changeField('object')}
              />

              <h3>Title</h3>
              <Input.TextArea
                disabled={!canParse(rule) || rule.isSaving}
                autosize={{minRows: 1}}
                value={fs ? fs.select.title : ''}
                onChange={changeField('title')}
              />

              <h3>Alert Time</h3>
              <Input.TextArea
                disabled={!canParse(rule) || rule.isSaving}
                autosize={{minRows: 1}}
                value={fs ? fs.select.alert_time : ''}
                onChange={changeField('alert_time')}
              />

              <h3>Description</h3>
              <Input.TextArea
                disabled={!canParse(rule) || rule.isSaving}
                autosize={{minRows: 1}}
                value={fs ? fs.select.description : ''}
                onChange={changeField('description')}
              />

              <h3>Event Data</h3>
              <Input.TextArea
                disabled={!canParse(rule) || rule.isSaving}
                autosize={{minRows: 1}}
                value={fs ? fs.select.event_data : ''}
                onChange={changeField('event_data')}
              />
            </Col>

            <Col span={12}>
              <h3>Detector</h3>
              <Input.TextArea
                disabled={!canParse(rule) || rule.isSaving}
                autosize={{minRows: 1}}
                value={fs ? fs.select.detector : ''}
                onChange={changeField('detector')}
              />

              <h3>Severity</h3>
              <Input.TextArea
                disabled={!canParse(rule) || rule.isSaving}
                autosize={{minRows: 1}}
                value={fs ? fs.select.severity : ''}
                onChange={changeField('severity')}
              />

              <h3>Query Id</h3>
              <Input.TextArea
                disabled={!canParse(rule) || rule.isSaving}
                autosize={{minRows: 1}}
                value={fs ? fs.select.query_id : ''}
                onChange={changeField('query_id')}
              />

              <h3>Query Name</h3>
              <Input.TextArea
                disabled={!canParse(rule) || rule.isSaving}
                autosize={{minRows: 1}}
                value={fs ? fs.select.query_name : ''}
                onChange={changeField('query_name')}
              />
            </Col>
            <Col span={24}>
              <h3>FROM</h3>
              <Input.TextArea
                disabled={!canParse(rule) || rule.isSaving}
                autosize={{minRows: 1}}
                value={fs ? fs.from : ''}
                onChange={changeField('', 'from')}
              />

              <h3>WHERE</h3>
              <Input.TextArea
                disabled={!canParse(rule) || rule.isSaving}
                value={fs ? fs.where : ''}
                spellCheck={false}
                autosize={{minRows: 1}}
                style={{fontFamily: 'Hack, monospace'}}
                onChange={changeField('', 'where')}
              />

              <h3>ENABLED</h3>
              <Checkbox checked={Boolean(fs && fs.enabled)} onChange={changeField('', 'enabled')}>
                Enabled
              </Checkbox>
            </Col>
          </div>
        );
      } else {
        form = (
          <div>
            <Col span={12}>
              <h3>Title</h3>
              <Input.TextArea
                disabled={!canParse(rule) || rule.isSaving}
                autosize={{minRows: 1}}
                value={fs ? fs.select.title : ''}
                onChange={changeField('title')}
              />

              <h3>Query Name</h3>
              <Input.TextArea
                disabled={!canParse(rule) || rule.isSaving}
                autosize={{minRows: 1}}
                value={fs ? fs.select.query_name : ''}
                onChange={changeField('query_name')}
              />

              <h3>Environment</h3>
              <Input.TextArea
                disabled={!canParse(rule) || rule.isSaving}
                autosize={{minRows: 1}}
                value={fs ? fs.select.environment : ''}
                onChange={changeField('environment')}
              />

              <h3>Sources</h3>
              <Input.TextArea
                disabled={!canParse(rule) || rule.isSaving}
                autosize={{minRows: 1}}
                value={fs ? fs.select.sources : ''}
                onChange={changeField('sources')}
              />

              <h3>Object</h3>
              <Input.TextArea
                disabled={!canParse(rule) || rule.isSaving}
                autosize={{minRows: 1}}
                value={fs ? fs.select.object : ''}
                onChange={changeField('object')}
              />

              <h3>Event Time</h3>
              <Input.TextArea
                disabled={!canParse(rule) || rule.isSaving}
                autosize={{minRows: 1}}
                value={fs ? fs.select.event_time : ''}
                onChange={changeField('event_time')}
              />
            </Col>

            <Col span={12}>
              <h3>Severity</h3>
              <Input.TextArea
                disabled={!canParse(rule) || rule.isSaving}
                autosize={{minRows: 1}}
                value={fs ? fs.select.severity : ''}
                onChange={changeField('severity')}
              />

              <h3>Alert Time</h3>
              <Input.TextArea
                disabled={!canParse(rule) || rule.isSaving}
                autosize={{minRows: 1}}
                value={fs ? fs.select.alert_time : ''}
                onChange={changeField('alert_time')}
              />

              <h3>Description</h3>
              <Input.TextArea
                disabled={!canParse(rule) || rule.isSaving}
                autosize={{minRows: 1}}
                value={fs ? fs.select.description : ''}
                onChange={changeField('description')}
              />

              <h3>Detector</h3>
              <Input.TextArea
                disabled={!canParse(rule) || rule.isSaving}
                autosize={{minRows: 1}}
                value={fs ? fs.select.detector : ''}
                onChange={changeField('detector')}
              />

              <h3>Event Data</h3>
              <Input.TextArea
                disabled={!canParse(rule) || rule.isSaving}
                autosize={{minRows: 1}}
                value={fs ? fs.select.event_data : ''}
                onChange={changeField('event_data')}
              />

              <h3>Actor</h3>
              <Input.TextArea
                disabled={!canParse(rule) || rule.isSaving}
                autosize={{minRows: 1}}
                value={fs ? fs.select.actor : ''}
                onChange={changeField('actor')}
              />

              <h3>Action</h3>
              <Input.TextArea
                disabled={!canParse(rule) || rule.isSaving}
                autosize={{minRows: 1}}
                value={fs ? fs.select.action : ''}
                onChange={changeField('action')}
              />
            </Col>

            <Col span={24}>
              <h3>FROM</h3>
              <Input.TextArea
                disabled={!canParse(rule) || rule.isSaving}
                autosize={{minRows: 1}}
                value={fs ? fs.from : ''}
                onChange={changeField('', 'from')}
              />

              <h3>WHERE</h3>
              <Input.TextArea
                disabled={!canParse(rule) || rule.isSaving}
                value={fs ? fs.where : ''}
                spellCheck={false}
                autosize={{minRows: 1}}
                style={{fontFamily: 'Hack, monospace'}}
                onChange={changeField('', 'where')}
              />

              <h3>ENABLED</h3>
              <Checkbox checked={Boolean(fs && fs.enabled)} onChange={changeField('', 'enabled')}>
                Enabled
              </Checkbox>
            </Col>
          </div>
        );
      }
    }

    // <h3>Query Id</h3>
    // <Input.TextArea
    //   disabled={!canParse(rule) || rule.isSaving}
    //   autosize={{minRows: 1}}
    //   value={fs ? fs.select.query_id : ''}
    //   onChange={changeField('query_id')}
    // />

    return (
      <div>
        {form}
        <Col span={24}>
          <Button
            type="primary"
            disabled={!canParse(rule) || rule.isSaving || rule.savedBody == rule.body}
            onClick={() => rule && this.props.saveRule(rule)}
          >
            {rule && rule.isSaving ? <Icon type="loading" theme="outlined" /> : <Icon type="upload" />} Apply
          </Button>
          <Button
            type="default"
            disabled={!canParse(rule) || rule.isSaving || rule.savedBody == rule.body}
            onClick={() => rule && changeRuleBody(rule.savedBody)}
          >
            <Icon type="rollback" theme="outlined" /> Revert
          </Button>
        </Col>
      </div>
    );
  }
}

const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators(
    {
      changeRuleBody,
      saveRule,
    },
    dispatch,
  );
};

const mapStateToProps = (state: State) => {
  return {
    rules: getRules(state),
  };
};

export default Object.assign(
  connect(
    mapStateToProps,
    mapDispatchToProps,
  )(FormEditor),
  {canParse},
);
