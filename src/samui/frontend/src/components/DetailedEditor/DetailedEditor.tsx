import {Input} from 'antd';
import * as React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';

import {getSnowAlertRules} from '../../reducers/rules';

import {State, SnowAlertRule, SnowAlertRulesState} from '../../reducers/types';

import './DetailedEditor.css';

interface OwnProps {}

interface DispatchProps {}

interface StateProps {
  rules: SnowAlertRulesState;
}

type DetailedEditorProps = OwnProps & DispatchProps & StateProps;

class DetailedEditor extends React.PureComponent<DetailedEditorProps> {
  populateField = (data?: SnowAlertRule['body']) => {
    const {TextArea} = Input;

    return <TextArea value={data} autosize={{minRows: 30, maxRows: 50}} style={{fontFamily: 'Hack, monospace'}} />;
  };

  render() {
    var ruleTitle = this.props.rules.currentRuleTitle;
    var rule = this.props.rules.rules.find(r => r.title == ruleTitle);
    console.log(rule);
    return this.populateField(rule && rule.body);
  }
}

const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators({}, dispatch);
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
