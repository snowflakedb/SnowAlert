import * as React from 'react';
import {connect} from 'react-redux';
import {bindActionCreators, Dispatch} from 'redux';
import {setViewport} from '../actions/viewport';

interface OwnProps {
  route: string;
  children: string | React.ReactNode;
  onClick?: (event: React.MouseEvent<HTMLAnchorElement>) => void;
  className?: string;
  style?: React.CSSProperties;
}

interface DispatchProps {
  setViewport: typeof setViewport;
}

type LinkProps = OwnProps & DispatchProps;

const onClick = (event: React.MouseEvent<HTMLAnchorElement>, props: LinkProps) => {
  // Disable href routing.
  event.preventDefault();

  if (props.onClick) {
    props.onClick(event);
  }
  props.setViewport(props.route);
};

const Link = (props: LinkProps) => (
  <a
    style={props.style}
    className={props.className}
    href={props.route}
    onClick={(event: React.MouseEvent<HTMLAnchorElement>) => onClick(event, props)}
  >
    {props.children}
  </a>
);

const mapStateToProps = () => {
  return {};
};

const mapDispatchToProps = (dispatch: Dispatch) => {
  return bindActionCreators(
    {
      setViewport,
    },
    dispatch,
  );
};

const ConnectedLink = connect<void, DispatchProps>(
  mapStateToProps,
  mapDispatchToProps,
)(Link);
export default ConnectedLink;
