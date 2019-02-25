import {Button, Icon, Input} from 'antd';
import * as React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';

import {getRules} from '../../reducers/rules';
import {updateRuleBody, saveRule, deleteRule} from '../../actions/rules';

import {State, SnowAlertRulesState} from '../../reducers/types';

import './RawEditor.css';

interface OwnProps {}

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
    const {currentRuleView, queries, suppressions} = this.props.rules;
    const rules = [...queries, ...suppressions];
    const rule = rules.find(r => r.view_name === currentRuleView);

    return (
      <div>
        <Input.TextArea
          disabled={!rule || rule.isSaving}
          value={rule ? rule.raw.body : ''}
          spellCheck={false}
          autosize={{minRows: 30}}
          onChange={e => this.props.updateRuleBody(e.target.value)}
        />
        <Button
          type="primary"
          disabled={!rule || rule.isSaving || !rule.isSaved || !rule.isEdited}
          onClick={() => rule && this.props.saveRule(rule.raw)}
        >
          {rule && rule.isSaving ? <Icon type="loading" theme="outlined" /> : <Icon type="upload" />} Apply
        </Button>
        <Button
          type="default"
          disabled={!rule || rule.isSaving || !rule.isSaved || !rule.isEdited}
          onClick={() => rule && this.props.updateRuleBody(rule.raw.savedBody)}
        >
          <Icon type="rollback" theme="outlined" /> Revert
        </Button>
        <Button
          type="default"
          disabled={!rule || rule.isSaving}
          onClick={() => rule && this.props.deleteRule(rule.raw)}
        >
          <Icon type="delete" theme="outlined" /> Delete
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

export default connect(
  mapStateToProps,
  mapDispatchToProps,
)(RawEditor);
