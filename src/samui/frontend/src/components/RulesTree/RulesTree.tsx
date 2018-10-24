import {Tree} from 'antd';
import * as React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';

import {loadSnowAlertRules, changeRule} from '../../actions/rules';
import {getSnowAlertRules} from '../../reducers/rules';

import {State, SnowAlertRule, SnowAlertRulesState} from '../../reducers/types';

import './RulesTree.css';

const TreeNode = Tree.TreeNode;

interface OwnProps {
  target: SnowAlertRule['target'];
}

interface DispatchProps {
  loadSnowAlertRules: typeof loadSnowAlertRules;
  changeRule: typeof changeRule;
}

interface StateProps {
  rules: SnowAlertRulesState;
}

type RulesTreeProps = OwnProps & DispatchProps & StateProps;

class RulesTree extends React.PureComponent<RulesTreeProps> {
  componentDidMount() {
    this.props.loadSnowAlertRules();
    this.props.changeRule('');
  }

  generateTree = (data: SnowAlertRulesState['rules'], target: SnowAlertRule['target']) => {
    const queryTitles: Array<string> = [];
    const suppressionTitles: Array<string> = [];

    for (let d of data)
      if (d.target === target) {
        if (d.type === 'query') {
          queryTitles.push(d.title.substr(0, d.title.length - (target.length + '_QUERY'.length + 1)));
        }
        if (d.type === 'suppression') {
          suppressionTitles.push(d.title.substr(0, d.title.length - (target.length + '_SUPPRESSION'.length + 1)));
        }
      }

    return [
      <TreeNode key="queries" title="Queries" selectable={false}>
        {this.props.rules.isFetching ? (
          <TreeNode title="Loading..." />
        ) : (
          queryTitles.map(x => <TreeNode key={`${x}_${target.toUpperCase()}_QUERY`} selectable title={x} />)
        )}
      </TreeNode>,
      <TreeNode key="suppressions" title="Suppressions" selectable={false}>
        {this.props.rules.isFetching ? (
          <TreeNode title="Loading..." />
        ) : (
          suppressionTitles.map(x => <TreeNode key={`${x}_${target.toUpperCase()}_SUPPRESSION`} selectable title={x} />)
        )}
      </TreeNode>,
    ];
  };

  render() {
    var rules = this.props.rules.rules;
    return (
      <Tree showLine defaultExpandAll onSelect={x => this.props.changeRule((x[0] || '').split('-')[0])}>
        {this.generateTree(rules, this.props.target)}
      </Tree>
    );
  }
}

const mapStateToProps = (state: State) => {
  return {
    rules: getSnowAlertRules(state),
  };
};

const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators(
    {
      loadSnowAlertRules: loadSnowAlertRules,
      changeRule,
    },
    dispatch,
  );
};

export default connect(
  mapStateToProps,
  mapDispatchToProps,
)(RulesTree);
