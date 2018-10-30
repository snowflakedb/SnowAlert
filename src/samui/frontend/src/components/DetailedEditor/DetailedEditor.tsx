import {Input} from 'antd';
import * as React from 'react';
import ReactDOM from 'react-dom';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';

import {currentRuleTitle} from '../../actions/rules';
import {getSnowAlertRules} from '../../reducers/rules';

import {State, SnowAlertRule, SnowAlertRulesState} from '../../reducers/types';

//import './DetailedEditor.css';

interface OwnProps {}

interface DispatchProps {}

interface StateProps {
  rules: SnowAlertRulesState;
  currentRuleTitle: string;
}

type DetailedEditorProps = OwnProps & DispatchProps & StateProps;

class DetailedEditor extends React.PureComponent<DetailedEditorProps> {
  //  componentDidMount() {
  //   this.props.changeCurrentRule();
  //  }

  populateField = (data?: SnowAlertRule['body']) => {
    const {TextArea} = Input;

    return <TextArea value={data} autosize={true} />;
  };

  render() {
    var ruleTitle = this.props.rules.currentRuleTitle;
    var rule = this.props.rules.rules.find(r => r.title == ruleTitle);
    console.log(rule);
    return this.populateField(rule && rule.body);
  }
}

const mapStateToProps = (state: State) => {
  return {
    rules: getSnowAlertRules(state),
  };
};

export default connect(
  mapStateToProps,
  //  mapDispatchToProps,
)(DetailedEditor);
