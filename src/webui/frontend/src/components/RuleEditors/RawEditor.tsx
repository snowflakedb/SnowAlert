import {Button, Input} from 'antd';
import {
  LoadingOutlined,
  UploadOutlined,
  RollbackOutlined,
  DeleteOutlined,
  CheckCircleOutlined,
} from '@ant-design/icons';

import * as React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';

import {getRules} from '../../reducers/rules';
import {updateRuleBody, saveRule, deleteRule} from '../../actions/rules';

import {State, SnowAlertRulesState} from '../../reducers/types';
import sqlFormatter from 'snowsql-formatter';

import './RawEditor.css';

interface OwnProps {
  currentRuleView: string | null;
}

interface DispatchProps {
  updateRuleBody: typeof updateRuleBody;
  saveRule: typeof saveRule;
  deleteRule: typeof deleteRule;
}

interface StateProps {
  rules: SnowAlertRulesState;
}

type RawEditorProps = OwnProps & DispatchProps & StateProps;

class RawEditor extends React.PureComponent<RawEditorProps> {
  render() {
    const {currentRuleView, deleteRule, saveRule, updateRuleBody} = this.props;
    const {queries, suppressions} = this.props.rules;
    const rules = [...queries, ...suppressions];
    const rule = rules.find((r) => r.viewName === currentRuleView);

    return (
      <div>
        <Input.TextArea
          disabled={!rule || rule.isSaving}
          value={rule ? rule.raw.body : ''}
          spellCheck={false}
          autoSize={{minRows: 30}}
          onChange={(e) => rule && updateRuleBody(rule.viewName, e.target.value)}
        />
        <Button
          type="primary"
          disabled={!rule || rule.isSaving || (rule.isSaved && !rule.isEdited)}
          onClick={() => rule && saveRule(rule)}
        >
          {rule && rule.isSaving ? <LoadingOutlined /> : <UploadOutlined />} Apply
        </Button>
        <Button
          type="default"
          disabled={!rule || rule.isSaving || (rule.isSaved && !rule.isEdited)}
          onClick={() => rule && updateRuleBody(rule.viewName, rule.raw.savedBody)}
        >
          <RollbackOutlined /> Revert
        </Button>
        <Button type="default" disabled={!rule || rule.isSaving} onClick={() => rule && deleteRule(rule.raw)}>
          <DeleteOutlined /> Delete
        </Button>
        <Button
          type="default"
          disabled={!rule || rule.isSaving}
          onClick={() => rule && updateRuleBody(rule.viewName, sqlFormatter.format(rule.raw.body))}
        >
          <CheckCircleOutlined /> Format
        </Button>
      </div>
    );
  }
}

const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators(
    {
      updateRuleBody,
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

export default connect(mapStateToProps, mapDispatchToProps)(RawEditor);
