import React from 'react';
import PropTypes from 'prop-types';


function Modal(props) {
  const visibleClassName = `modal modal-${props.show ? 'visible' : 'hidden'}`
  return (
    <div className={visibleClassName} tabIndex="-1" role="dialog" aria-labelledby="exampleModalLabel" aria-hidden="true">
      <div className="modal-dialog modal-lg modal-dialog-centered modal-scrollable" role="document">
        <div className="modal-content shadow">
          <div className="modal-header">
            <h5 className="modal-title" id="exampleModalLabel">{props.title}</h5>
            <button type="button" className="close" data-dismiss="modal" aria-label="Close" onClick={props.onClose}>
              <span aria-hidden="true">&times;</span>
            </button>
          </div>
          <div className="modal-body">
            {props.body}
          </div>
          <div className="modal-footer">
            <button type="button" className="btn btn-primary" data-dismiss="modal" onClick={props.onClose}>OK</button>
          </div>
        </div>
      </div>
    </div>
  )
}

Modal.propTypes = {
  show: PropTypes.any,
  title: PropTypes.any,
  body: PropTypes.any,
  onClose: PropTypes.func,
}

export default Modal;