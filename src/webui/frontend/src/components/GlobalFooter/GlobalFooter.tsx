import classNames from 'classnames';
import * as React from 'react';
import './GlobalFooter.css';

interface GlobalFooterProps {
  className?: string;
  links?: ReadonlyArray<{
    key: string;
    href: string;
    title: string;
    blankTarget?: boolean;
  }>;
  copyright: string | React.ReactNode;
}

const GlobalFooter = (props: GlobalFooterProps) => {
  const {className, links, copyright} = props;

  const clsString = classNames('global-footer', className);
  return (
    <div className={clsString}>
      {links && (
        <div className={'links'}>
          {links.map((link) => (
            <a key={link.key} target={link.blankTarget ? '_blank' : '_self'} href={link.href}>
              {link.title}
            </a>
          ))}
        </div>
      )}
      {copyright && <div className={'copyright'}>{copyright}</div>}
    </div>
  );
};

export default GlobalFooter;
