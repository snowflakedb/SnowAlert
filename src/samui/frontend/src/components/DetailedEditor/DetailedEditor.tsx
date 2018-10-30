import {Input} from 'antd';
import * as React from 'react';
import ReactDOM from 'react-dom';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';

import {currentQuery} from '../../actions/rules';
import {getSnowAlertRules} from '../../reducers/rules';

import {State, SnowAlertRule, SnowAlertRulesState} from '../../reducers/types';

import './DetailedEditor.css';

interface OwnProps {}

interface DispatchProps {
  changeCurrentQuery: typeof currentQuery;
}

interface StateProps {
  currentQuery: SnowAlertRule;
}

type DetailedEditorProps = OwnProps & DispatchProps & StateProps;

class DetailedEditor extends React.PureComponent<DetailedEditorProps> {
  componentDidMount() {
    this.props.changeCurrentQuery();
  }

  populateField = (data: SnowAlertRule['body']) => {
    const {TextArea} = Input;

    return <TextArea value={data} autosize={true} />;
  };

  render() {
    var rule = this.props.currentQuery;
    return this.populateField(rule.body);
  }
}

export default DetailedEditor;
