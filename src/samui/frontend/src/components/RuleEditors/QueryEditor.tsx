import {Button, Col, Icon, Input, Switch} from 'antd';
import * as React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';

import {getRules} from '../../reducers/rules';
import {Query} from '../../store/rules';
import {updateRuleBody, updateRule, saveRule, deleteRule} from '../../actions/rules';

import {State, SnowAlertRulesState} from '../../reducers/types';

import './QueryEditor.css';

interface stringFieldsDefinition {
  title: string;
  type: 'string' | 'text';
  getValue(r: any): string;
  setValue(q: Query, v: string): Query;
}

interface boolFieldDefinition {
  title: string;
  type: 'boolean';
  getValue(r: any): boolean;
  setValue(q: Query, v: boolean): Query;
}

interface OwnProps {
  cols: {
    span: number;
    fields: (stringFieldsDefinition | boolFieldDefinition)[];
  }[];
}

interface DispatchProps {
  updateRuleBody: typeof updateRuleBody;
  updateRule: typeof updateRule;
  saveRule: typeof saveRule;
  deleteRule: typeof deleteRule;
}

interface StateProps {
  rules: SnowAlertRulesState;
}

type QueryEditorProps = OwnProps & DispatchProps & StateProps;

class QueryEditor extends React.PureComponent<QueryEditorProps> {
  render() {
    const {updateRule, updateRuleBody, cols, saveRule} = this.props;
    const {currentRuleView, rules, queries} = this.props.rules;
    const q = queries.find(q => q.view_name === currentRuleView);

    if (!(currentRuleView && q && q instanceof Query && q.isParsed)) {
      return (
        <Col span={16}>
          <h3>Loaded {rules.length} rules from Snowflake.</h3>
        </Col>
      );
    }

    return (
      <div>
        {cols.map((col, i) => (
          <Col key={`col-${i}`} span={col.span}>
            {col.fields.map(
              (field, i) =>
                field.type === 'string' ? (
                  <div key={`col-${i}`}>
                    <h3>{field.title}</h3>
                    <Input.TextArea
                      disabled={q.isSaving}
                      spellCheck={false}
                      autosize={{minRows: 1}}
                      value={field.getValue(q)}
                      onChange={e => updateRule(currentRuleView, field.setValue(q, e.target.value))}
                    />
                  </div>
                ) : field.type === 'text' ? (
                  <div key={`col-${i}`}>
                    <h3>{field.title}</h3>
                    <Input.TextArea
                      disabled={q.isSaving}
                      spellCheck={false}
                      autosize={{minRows: 1}}
                      value={field.getValue(q)}
                      onChange={e => updateRule(currentRuleView, field.setValue(q, e.target.value))}
                    />
                  </div>
                ) : field.type === 'boolean' ? (
                  <div key={`col-${i}`}>
                    <Switch
                      disabled={q.isSaving}
                      defaultChecked={field.getValue(q)}
                      onChange={e => updateRule(currentRuleView, field.setValue(q, e))}
                    >
                      {field.title}
                    </Switch>
                  </div>
                ) : null,
            )}
          </Col>
        ))}
        <Col span={24}>
          {console.log(`isSaving=${q.isSaving} isSaved=${q.isSaved} isEdited=${q.isEdited}`)}
          <Button
            type="primary"
            disabled={q.isSaving || !q.isSaved || !q.isEdited}
            onClick={() => q && saveRule(q.raw)}
          >
            {q && q.isSaving ? <Icon type="loading" theme="outlined" /> : <Icon type="upload" />} Apply
          </Button>
          <Button
            type="default"
            disabled={q.isSaving || !q.isSaved || !q.isEdited}
            onClick={() => q && updateRuleBody(q.raw.savedBody)}
          >
            <Icon type="rollback" theme="outlined" /> Revert
          </Button>
          <Button type="default" disabled={q.isSaving || !q.isSaved} onClick={() => q && this.props.deleteRule(q.raw)}>
            <Icon type="delete" theme="outlined" /> Delete
          </Button>
        </Col>
      </div>
    );
  }
}

const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators(
    {
      updateRuleBody,
      updateRule,
      saveRule,
      deleteRule,
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
  )(QueryEditor),
);
