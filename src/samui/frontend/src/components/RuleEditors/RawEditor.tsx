import {Button, Icon, Input} from 'antd';
import * as React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';

import {getRules} from '../../reducers/rules';
import {changeRuleBody, saveRule, deleteRule} from '../../actions/rules';

import {State, SnowAlertRulesState} from '../../reducers/types';

import './RawEditor.css';

interface OwnProps {}

interface DispatchProps {
  changeRuleBody: typeof changeRuleBody;
  saveRule: typeof saveRule;
  deleteRule: typeof deleteRule;
}

interface StateProps {
  rules: SnowAlertRulesState;
}

type RawEditorProps = OwnProps & DispatchProps & StateProps;

class RawEditor extends React.PureComponent<RawEditorProps> {
  render() {
    const {currentRuleView, rules} = this.props.rules;
    const rule = rules.find(r => `${r.title}_${r.target}_${r.type}` == currentRuleView);

    return (
      <div>
        <Input.TextArea
          disabled={!rule || rule.isSaving}
          value={rule ? rule.body : ''}
          spellCheck={false}
          autosize={{minRows: 30, maxRows: 50}}
          onChange={e => this.props.changeRuleBody(e.target.value)}
        />
        <Button
          type="primary"
          disabled={!rule || rule.isSaving || rule.savedBody == rule.body}
          onClick={() => rule && this.props.saveRule(rule)}
        >
          {rule && rule.isSaving ? <Icon type="loading" theme="outlined" /> : <Icon type="upload" />} Apply
        </Button>
        <Button
          type="default"
          disabled={!rule || rule.isSaving || rule.savedBody == rule.body}
          onClick={() => rule && this.props.changeRuleBody(rule.savedBody)}
        >
          <Icon type="rollback" theme="outlined" /> Revert
        </Button>
        <Button type="default" disabled={!rule || rule.isSaving} onClick={() => rule && this.props.deleteRule(rule)}>
          <Icon type="delete" theme="outlined" /> Delete
        </Button>
      </div>
    );
  }
}

const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators(
    {
      changeRuleBody,
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
