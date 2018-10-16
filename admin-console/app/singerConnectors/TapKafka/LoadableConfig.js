/**
 * Asynchronously loads the component for TapKafka
 */
import Loadable from 'react-loadable';

import LoadingIndicator from 'components/LoadingIndicator';

export default Loadable({
  loader: () => import('./Config'),
  loading: LoadingIndicator,
});
