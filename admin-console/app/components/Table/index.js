import React from 'react';
import PropTypes from 'prop-types';

function Table(props) {
  const HeaderComponentToRender = props.headerComponent;
  const BodyComponentToRender = props.bodyComponent;
  const headerContent = <thead className="thead-light"><HeaderComponentToRender other={props.other}/></thead>
  let bodyContent = <tbody />

  // If we have items, render them
  if (props.items) {
    bodyContent = props.items.map((item, i) => (
      <BodyComponentToRender
        key={`flow-item-${i}`}
        item={item}
        selectedItem={props.selectedItem}
        onItemSelect={props.onItemSelect}
        delegatedProps={props.delegatedProps}
      />
    ));
  } else {
    // Otherwise render a single component
    bodyContent = <tbody><BodyComponentToRender /></tbody>;
  }

  return (
    <table className="table table-hover">
      {headerContent}
      <tbody>{bodyContent}</tbody>
    </table>
  );
}

Table.propTypes = {
  items: PropTypes.array,
  selectedItem: PropTypes.any,
  headerComponent: PropTypes.func.isRequired,
  bodyComponent: PropTypes.func.isRequired,
  onItemSelect: PropTypes.func,
  delegatedProps: PropTypes.any,
}

export default Table;