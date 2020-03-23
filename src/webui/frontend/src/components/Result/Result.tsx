import {CloseCircleOutlined, CheckCircleOutlined} from '@ant-design/icons';
import classNames from 'classnames';
import * as React from 'react';
import './Result.css';

interface Props {
  className: string;
  type: string;
  title: string | React.ReactNode;
  description: string;
  extra?: React.ReactNode;
  actions?: React.ReactNode;
  style?: React.CSSProperties;
}

const Result = (props: Props) => {
  const {className, type, title, description, extra, actions} = props;

  const iconMap: any = {
    error: <CloseCircleOutlined className={'error'} />,
    success: <CheckCircleOutlined className={'success'} />,
  };
  const clsString = classNames('result', className);
  return (
    <div className={clsString}>
      <div className={'result-icon'}>{iconMap[type]}</div>
      <div className={'result-title'}>{title}</div>
      {description && <div className={'result-description'}>{description}</div>}
      {extra && <div className={'result-extra'}>{extra}</div>}
      {actions && <div className={'result-actions'}>{actions}</div>}
    </div>
  );
};

export default Result;
