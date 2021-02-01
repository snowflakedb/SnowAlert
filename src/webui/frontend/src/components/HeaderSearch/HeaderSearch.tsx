import {AutoComplete, Input} from 'antd';
import {SearchOutlined} from '@ant-design/icons';
import classNames from 'classnames';
import * as React from 'react';
import './HeaderSearch.css';

interface Props {
  className: string;
  placeholder: string;
  onPressEnter: (value: string) => void;
  onChange?: () => void;
}

interface State {
  searchMode: boolean;
  value: string;
}

export default class HeaderSearch extends React.PureComponent<Props, State> {
  state = {
    searchMode: false,
    value: '',
  };

  input: Input | null = null;
  timeout: number = 0;

  componentWillUnmount() {
    if (this.timeout) {
      clearTimeout(this.timeout);
    }
  }

  onKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      this.timeout = window.setTimeout(() => {
        // Fix duplicate onPressEnter.
        this.props.onPressEnter(this.state.value);
      }, 0);
    }
  };

  onChange = (value: string) => {
    this.setState({value});
    if (this.props.onChange) {
      this.props.onChange();
    }
  };

  enterSearchMode = () => {
    this.setState({searchMode: true}, () => {
      if (this.state.searchMode && this.input) {
        this.input.focus();
      }
    });
  };

  leaveSearchMode = () => {
    this.setState({
      searchMode: false,
      value: '',
    });
  };

  render() {
    const {className, placeholder, ...restProps} = this.props;
    const inputClass = classNames('input', {
      show: this.state.searchMode,
    });
    return (
      <span className={classNames('header-search', className)} onClick={this.enterSearchMode}>
        <SearchOutlined key="Icon" />
        <AutoComplete
          dataSource={[]}
          key="AutoComplete"
          {...restProps}
          className={inputClass}
          value={this.state.value}
          onChange={(e) => this.onChange(String(e))}
        >
          <Input
            placeholder={placeholder}
            ref={(node: Input | null) => {
              this.input = node;
            }}
            onKeyDown={this.onKeyDown}
            onBlur={this.leaveSearchMode}
          />
        </AutoComplete>
      </span>
    );
  }
}
