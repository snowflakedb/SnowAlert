import * as React from 'react';
import {Component} from 'react';

interface SearchBoxProps {
  query: string;
  changeSearch: (query: string)=>{}
}
interface SearchBoxState {
  query: string;
}

export default class SearchBox extends Component<SearchBoxProps, SearchBoxState> {

  handleChange(event: React.SyntheticEvent) {
    const target = event.target as HTMLInputElement;
    this.props.changeSearch(target.value);
  }

  render() {
    return (
      <div className="search-box">
        <input
          type="search"
          placeholder="Search..."
          value={this.props.query}
          onChange={this.handleChange.bind(this)}/>
      </div>
    );
  }
}
