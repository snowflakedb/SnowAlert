import * as React from 'react';
import {Tag, Input, Tooltip} from 'antd';
import {PlusOutlined} from '@ant-design/icons';

interface Props {
  disabled: boolean;
  tags: string;
  onChange: (e: string) => void;
}

interface State {
  inputVisible: boolean;
  inputValue: '';
}

export default class EditableTagGroup extends React.Component<Props, State> {
  input: any;

  constructor(props: Props) {
    super(props);
    this.state = {
      inputVisible: false,
      inputValue: '',
    };
  }

  get tags() {
    const t = this.props.tags;
    return t.length ? t.split(', ') : [];
  }

  handleClose = (removedTag: string) => {
    if (this.props.disabled) {
      return;
    }
    const tags = this.tags.filter((tag: string) => tag !== removedTag);
    this.props.onChange(tags.join(', '));
  };

  showInput = () => {
    this.setState({inputVisible: true}, () => this.input.focus());
  };

  handleInputChange = (e: any) => {
    this.setState({inputValue: e.target.value});
  };

  handleInputConfirm = () => {
    const state = this.state;
    const inputValue = state.inputValue;
    let tags = this.tags;
    if (inputValue && tags.indexOf(inputValue) === -1) {
      tags = [...tags, inputValue];
    }
    this.props.onChange(tags.join(', '));
    this.setState({
      inputVisible: false,
      inputValue: '',
    });
  };

  saveInputRef = (input: any) => (this.input = input);

  render() {
    const {inputVisible, inputValue} = this.state;
    return (
      <div>
        {this.tags.map((tag: string, index: number) => {
          const isLongTag = tag.length > 20;
          const tagElem = (
            <Tag key={tag} closable={true} onClose={() => this.handleClose(tag)}>
              {isLongTag ? `${tag.slice(0, 20)}...` : tag}
            </Tag>
          );
          return isLongTag ? (
            <Tooltip title={tag} key={tag}>
              {tagElem}
            </Tooltip>
          ) : (
            tagElem
          );
        })}
        {inputVisible && (
          <Input
            ref={this.saveInputRef}
            type="text"
            size="small"
            style={{width: 78}}
            value={inputValue}
            onChange={this.handleInputChange}
            onBlur={this.handleInputConfirm}
            onPressEnter={this.handleInputConfirm}
          />
        )}
        {!inputVisible && (
          <Tag onClick={this.showInput} style={{background: '#fff', borderStyle: 'dashed'}}>
            <PlusOutlined /> New Tag
          </Tag>
        )}
      </div>
    );
  }
}
