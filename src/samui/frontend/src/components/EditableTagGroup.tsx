import * as React from 'react';
import {Tag, Input, Tooltip, Icon} from 'antd';

export default class EditableTagGroup extends React.Component<any, any> {
  input: any;

  constructor(props: any) {
    super(props);
    this.state = {
      tags: props.defaultTags.split(', ') || [],
      inputVisible: false,
      inputValue: '',
    };
  }

  handleClose = (removedTag: string) => {
    const tags = this.state.tags.filter((tag: string) => tag !== removedTag);
    this.setState({tags});
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
    let tags = state.tags;
    if (inputValue && tags.indexOf(inputValue) === -1) {
      tags = [...tags, inputValue];
    }
    this.props.onChange(tags.join(', '));
    this.setState({
      tags,
      inputVisible: false,
      inputValue: '',
    });
  };

  saveInputRef = (input: any) => (this.input = input);

  render() {
    const {tags, inputVisible, inputValue} = this.state;
    console.log(tags);
    return (
      <div>
        {tags.map((tag: string, index: number) => {
          const isLongTag = tag.length > 20;
          const tagElem = (
            <Tag key={tag} closable={true} afterClose={() => this.handleClose(tag)}>
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
            <Icon type="plus" /> New Tag
          </Tag>
        )}
      </div>
    );
  }
}

//ReactDOM.render(<EditableTagGroup onChange={console.log} defaultTags={['a']} />, mountNode);
