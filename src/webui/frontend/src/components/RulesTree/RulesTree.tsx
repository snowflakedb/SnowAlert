import {Button, Icon, Tree, Input} from 'antd';
import * as React from 'react';
import * as _ from 'lodash';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';

import {navigate} from '../../store/history';

import {loadSnowAlertRules, changeRule, changeFilter} from '../../actions/rules';
import {getRules} from '../../reducers/rules';

import {State, SnowAlertRule, SnowAlertRulesState} from '../../reducers/types';
import {Query, Suppression} from '../../store/rules';

import './RulesTree.css';

const TreeNode = Tree.TreeNode;
const Search = Input.Search;

function download(filename: string, text: string) {
  const element = document.createElement('a');
  element.setAttribute('href', `data:text/plain;charset=utf-8,${encodeURIComponent(text)}`);
  element.setAttribute('download', filename);
  element.style.display = 'none';
  document.body.appendChild(element);
  element.click();
  document.body.removeChild(element);
}

function allMatchedCaptures(regexp: RegExp, s: string): string[][] {
  const matches: string[][] = [];
  s.replace(regexp, (m, ...args) => {
    matches.push(args.slice(0, args.length - 2));
    return '';
  });
  return matches;
}

interface OwnProps {
  currentRuleView: string | null;
  target: SnowAlertRule['target'];
}

interface DispatchProps {
  loadSnowAlertRules: typeof loadSnowAlertRules;
  changeRule: typeof changeRule;
  changeFilter: typeof changeFilter;
}

interface StateProps {
  rules: SnowAlertRulesState;
}

type RulesTreeProps = OwnProps & DispatchProps & StateProps;

class RulesTree extends React.PureComponent<RulesTreeProps> {
  componentDidMount() {
    this.props.loadSnowAlertRules();
  }

  generateTree = (
    queries: ReadonlyArray<Query>,
    suppressions: ReadonlyArray<Suppression>,
    target: SnowAlertRule['target'],
    filter: string,
  ) => {
    function ruleFilter(rule: Query | Suppression) {
      const filterTags = _.flatten(allMatchedCaptures(/tag\[([^\]]+)\]/g, filter));
      return (
        filter === '' ||
        rule.viewName.includes(filter.toUpperCase()) ||
        rule.raw.body.toUpperCase().includes(filter.toUpperCase()) ||
        (filterTags.length > 0 && _.intersection(rule.tags, filterTags).length === filterTags.length)
      );
    }

    return [
      <TreeNode key="queries" title={`${target} Queries`} selectable={false}>
        {this.props.rules.isFetching ? (
          <TreeNode title="Loading..." />
        ) : (
          queries
            .filter(q => target === q.target)
            .filter(ruleFilter)
            .map(r => (
              <TreeNode
                selectable={true}
                key={`${r.viewName}`}
                title={(r.isSaving ? '(saving) ' : r.isEdited ? '* ' : '') + r.title}
              />
            ))
        )}
      </TreeNode>,
      <TreeNode key="suppressions" title={`${target} Suppressions`} selectable={false}>
        {this.props.rules.isFetching ? (
          <TreeNode title="Loading..." />
        ) : (
          suppressions
            .filter(s => target === s.target)
            .filter(ruleFilter)
            .map(s => (
              <TreeNode
                selectable={true}
                key={s.viewName}
                title={(s.isSaving ? '(saving) ' : s.isEdited ? '* ' : '') + s.title}
              />
            ))
        )}
      </TreeNode>,
    ];
  };

  render() {
    const {
      currentRuleView,
      changeFilter,
      target,
      rules: {queries, suppressions, filter},
    } = this.props;
    return (
      <div>
        <Search
          style={{width: 200}}
          placeholder={`${target} Query Name`}
          value={filter}
          onChange={e => changeFilter(e.target.value)}
        />
        <Tree
          showLine
          defaultExpandAll
          onSelect={x => navigate(x[0] || '.')}
          selectedKeys={currentRuleView ? [currentRuleView] : []}
        >
          {this.generateTree(queries, suppressions, target, filter)}
        </Tree>
        <Button
          type="dashed"
          disabled={queries.length === 0}
          onClick={() => {
            download(
              `${new Date().toISOString().replace(/[:.]/g, '')}-backup.sql`,
              [...queries, ...suppressions].map(q => q.raw.body).join('\n\n'),
            );
          }}
        >
          <Icon type="cloud-download" theme="outlined" /> Download SQL
        </Button>
        <Button type="dashed" disabled={true}>
          <Icon type="cloud-upload" theme="outlined" /> Upload SQL
        </Button>
      </div>
    );
  }
}

const mapStateToProps = (state: State) => {
  return {
    rules: getRules(state),
  };
};

const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators(
    {
      loadSnowAlertRules,
      changeRule,
      changeFilter,
    },
    dispatch,
  );
};

export default connect(mapStateToProps, mapDispatchToProps)(RulesTree);
