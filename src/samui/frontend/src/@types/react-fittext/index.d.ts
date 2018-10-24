declare module 'react-fittext' {
  import * as React from 'react';

  interface ReactFitTextProps {
    children: JSX.Element;
    compressor?: number;
    minFontSize?: number;
    maxFontSize?: number;
  }

  export default class ReactFitText extends React.Component<ReactFitTextProps, any> {}
}
