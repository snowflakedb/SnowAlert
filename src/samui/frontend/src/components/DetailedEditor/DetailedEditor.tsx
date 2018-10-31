import {Input} from 'antd';
import * as React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';

import {getSnowAlertRules} from '../../reducers/rules';
import {changeRuleBody} from '../../actions/rules';

import {State, SnowAlertRule, SnowAlertRulesState} from '../../reducers/types';

import './DetailedEditor.css';

interface OwnProps {}

interface DispatchProps {
  changeRuleBody: typeof changeRuleBody;
}

interface StateProps {
  rules: SnowAlertRulesState;
}

type DetailedEditorProps = OwnProps & DispatchProps & StateProps;

class DetailedEditor extends React.PureComponent<DetailedEditorProps> {
  populateField = (editorBody?: SnowAlertRule['body']) => {
    const {TextArea} = Input;

    return (
      <TextArea
        disabled={!editorBody}
        value={editorBody}
        autosize={{minRows: 30, maxRows: 50}}
        style={{fontFamily: 'Hack, monospace'}}
        onChange={e => this.props.changeRuleBody(e.target.value)}
      />
    );
  };

  render() {
    const rules = this.props.rules;
    const ruleTitle = rules.currentRuleTitle;
    const rule = rules.rules.find(r => r.title == ruleTitle);
    return this.populateField(rule && rule.body);
  }
}

const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators(
    {
      changeRuleBody,
    },
    dispatch,
  );
};

const mapStateToProps = (state: State) => {
  return {
    rules: getSnowAlertRules(state),
  };
};

export default connect(
  mapStateToProps,
  mapDispatchToProps,
)(DetailedEditor);
