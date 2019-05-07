import {G2} from 'bizcharts';
import Pie from './Pie';

G2.track(false);

const config = G2.Util.deepMix(
  {
    defaultColor: '#1089ff',
    shape: {
      interval: {
        fillOpacity: 1,
      },
    },
  },
  G2.Global,
);

G2.Global.setTheme(config);

export {Pie};
