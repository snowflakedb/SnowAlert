import {DataView} from '@antv/data-set';
import {Divider} from 'antd';
import {Chart, Coord, Geom, Tooltip} from 'bizcharts';
import classNames from 'classnames';
import {Bind, Debounce} from 'lodash-decorators';
import * as React from 'react';
import ReactFitText from 'react-fittext';
import './Pie.css';

interface LegendItem {
  checked: boolean;
  color: string;
  percent: number;
  x: string;
  y: string;
}

interface PieProps {
  valueFormat?: (value: string) => string;
  subTitle?: string;
  total?: number;
  hasLegend?: boolean;
  className?: string;
  style?: React.CSSProperties;
  height: number;
  forceFit?: boolean;
  percent?: number;
  color?: string;
  inner: number;
  animate?: boolean;
  colors?: string[];
  lineWidth?: number;
  selected?: boolean;
  tooltip?: boolean;
  data?: ReadonlyArray<{
    x: string;
    y: number;
  }>;
}

interface PieState {
  legendData: LegendItem[];
  legendBlock: boolean;
}

export default class PieComponent extends React.Component<PieProps, PieState> {
  state: PieState = {
    legendBlock: false,
    legendData: [],
  };

  chart: G2.Chart;
  root: HTMLElement | null;

  componentDidMount() {
    this.getLegendData();
    this.resize();
    window.addEventListener('resize', this.resize);
  }

  componentWillUnmount() {
    window.removeEventListener('resize', this.resize);
  }

  getG2Instance = (chart: G2.Chart) => {
    this.chart = chart;
  };

  // For custom legend view.
  getLegendData = () => {
    if (!this.chart) {
      return;
    }
    const geom = this.chart.getAllGeoms()[0];
    const items: ReadonlyArray<LegendItem> = geom.get('dataArray') || [];

    const legendData = items.map(item => {
      /* eslint no-underscore-dangle:0 */
      const origin = item[0]._origin;
      origin.color = item[0].color;
      origin.checked = true;
      return origin;
    });

    this.setState({
      legendData,
    });
  };

  // For window resize auto responsive legend.
  @Bind()
  @Debounce(300)
  resize() {
    const {hasLegend} = this.props;
    if (!hasLegend || !this.root) {
      window.removeEventListener('resize', this.resize);
      return;
    }
    if (this.root.parentElement && this.root.parentElement.clientWidth <= 380) {
      if (!this.state.legendBlock) {
        this.setState({
          legendBlock: true,
        });
      }
    } else if (this.state.legendBlock) {
      this.setState({
        legendBlock: false,
      });
    }
  }

  handleRoot = (n: HTMLElement | null) => {
    this.root = n;
  };

  handleLegendClick = (item: LegendItem, i: number) => {
    const newItem = item;
    newItem.checked = !newItem.checked;

    const {legendData} = this.state;
    legendData[i] = newItem;

    const filteredLegendData = legendData.filter(l => l.checked).map(l => l.x);

    if (this.chart) {
      this.chart.filter('x', (val: string) => filteredLegendData.indexOf(val) > -1);
    }

    this.setState({
      legendData,
    });
  };

  render() {
    const {
      valueFormat,
      subTitle,
      total,
      hasLegend = false,
      className,
      style,
      height,
      forceFit = true,
      percent = 0,
      color,
      inner = 0.75,
      animate = true,
      colors,
      lineWidth = 1,
    } = this.props;

    const {legendData, legendBlock} = this.state;
    const pieClassName = classNames('pie', className, {
      'has-legend': hasLegend,
      'legend-block': legendBlock,
    });

    const defaultColors = colors || ['#F0F2F5'];
    let data = this.props.data || [];
    let selected = this.props.selected || true;
    let tooltip = this.props.tooltip || true;

    const formatColor = (d?: any) => {
      if (d === 'primary') {
        return color || 'rgba(24, 144, 255, 0.85)';
      } else {
        return '#F0F2F5';
      }
    };

    if (percent) {
      selected = false;
      tooltip = false;
      data = [
        {
          x: 'primary',
          y: percent,
        },
        {
          x: 'rest',
          y: 100 - percent,
        },
      ];
    }

    const tooltipFormat: [string, (x: string, y: number) => {name: string; value: string}] = [
      'x*percent',
      (x: string, y: number) => ({
        name: x,
        value: `${(y * 100).toFixed(2)}%`,
      }),
    ];

    const padding = {
      top: 12,
      right: 0,
      bottom: 12,
      left: 0,
    };

    const dv = new DataView();
    dv.source(data).transform({
      as: 'percent',
      dimension: 'x',
      field: 'y',
      type: 'percent',
    });

    return (
      <div ref={this.handleRoot} className={pieClassName} style={style}>
        <ReactFitText maxFontSize={25}>
          <div className={'chart'}>
            <Chart
              height={height}
              forceFit={forceFit}
              data={dv}
              padding={padding}
              animate={animate}
              onGetG2Instance={this.getG2Instance}
            >
              {tooltip && <Tooltip showTitle={false} />}
              <Coord type="theta" innerRadius={inner} />
              <Geom
                style={{lineWidth, stroke: '#fff'}}
                tooltip={tooltip && tooltipFormat}
                type="intervalStack"
                position="percent"
                color={percent ? ['x', formatColor] : ['x', defaultColors]}
                select={selected}
              />
            </Chart>

            {(subTitle || total) && (
              <div className={'total'}>
                {subTitle && <h4 className="pie-sub-title">{subTitle}</h4>}
                {/* tslint:disable-next-line */}
                {total && <div className="pie-stat" dangerouslySetInnerHTML={{__html: total.toString()}} />}
              </div>
            )}
          </div>
        </ReactFitText>

        {hasLegend && (
          <ul className={'legend'}>
            {legendData.map((item, i) => (
              <li key={item.x} onClick={() => this.handleLegendClick(item, i)}>
                <span
                  className={'dot'}
                  style={{
                    backgroundColor: !item.checked ? '#aaa' : item.color,
                  }}
                />
                <span className={'legendTitle'}>{item.x}</span>
                <Divider type="vertical" />
                <span className={'percent'}>{`${(isNaN(item.percent) ? 0 : item.percent * 100).toFixed(2)}%`}</span>
                <span className={'value'}>{valueFormat ? valueFormat(item.y) : item.y}</span>
              </li>
            ))}
          </ul>
        )}
      </div>
    );
  }
}
