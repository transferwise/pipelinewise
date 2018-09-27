import { injectGlobal } from 'styled-components';

/* eslint no-unused-expressions: 0 */
injectGlobal`
  * {
    font-size: 14px;
  }
  .font-sm {
    font-size: 60%;
  }
  html {
    position: relative;
    min-height: 100%;
    background-color: #fafafa;
  }
  body {
    font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;
    margin-bottom: 60px; /* Margin bottom by footer height */
  }
  .container a {
    color: #2f4366 !important;
  }
  body main.container-fluid {
    padding: 60px 15px 80px 0;
  }
  body main.container {
    padding: 60px 15px 80px 0;
  }
  body.fontLoaded {
    font-family: 'Open Sans', 'Helvetica Neue', Helvetica, Arial, sans-serif;
  }
  .bg-blue {
    background-color: #37517e !important;
  }
  .table-wrapper-scroll-y {
    display: block;
    max-height: 300px;
    overflow-y: auto;
    -ms-overflow-style: -ms-autohiding-scrollbar;
  }
  .overlay {
    background: #e9e9e9;
    display: none;
    position: absolute;
    top: 0;
    right: 0;
    bottom: 0;
    left: 0;
    opacity: 0.5;
  }
  .modal-visible {
    display: initial !important;
  }
  .modal-hidden {
    display: none !important;
  }
  .modal-dialog {
    overflow-y: initial !important;
  }
  .modal-body {
    max-height: 650px;
    overflow-y: auto;
  }
  .syntax-highligher-scrollable {
    max-height: 500px;
    overflow-y: auto;
  }
  .arrow-right {
    width: 0; 
    height: 0; 
    border-top: 60px solid transparent;
    border-bottom: 60px solid transparent;
    
    border-left: 60px solid green;
  }
  #app {
    background-color: #fafafa;
    min-height: 100%;
    min-width: 100%;
  }
  .img-icon {
    height: 40px;
  }
  .img-icon-md {
    height: 28px;
  }
  .img-icon-sm {
    height: 14px;
  }
  td.flow-item:hover {
    background: #f0f0f0;
    box-shadow: 5px 5px 20px rgba(0,0,0,0.15);
  }
  td.flow-item-active {
    background: #bbe0bb;
    box-shadow: 5px 5px 20px rgba(0,0,0,0.15);
  }
  td.flow-item-active:hover {
    background: #ccf0cc;
    box-shadow: 5px 5px 20px rgba(0,0,0,0.15);
  }
  .react-tabs__tab--selected {
    background: transparent !important;
    border-radius: 0 !important;
    border-color: transparent !important;
    border-bottom: 3px solid orange !important;
  }
  .react-tabs__tab:hover {
    background: transparent !important;
    border-radius: 0 !important;
    border-color: transparent !important;
    border-bottom: 3px solid gray !important;
  }
  .react-tabs__tab--selected {
    border-bottom: inherit;
    border-width: thick;
    border-radius: 0;
  }
  .react-tabs__tab--vertical {
    display: table;
    color: #829ca9;
    border: 1px solid transparent;
    border-bottom: none;
    bottom: -1px;
    position: relative;
    list-style: none;
    padding: 6px 12px;
    cursor: pointer;
    width: 100%;
  }
  .react-tabs__tab--vertical:hover {
    background: lightgray;
    box-shadow: 2px 2px 5px rgba(0,0,0,0);
    width: 100%
  }
  .react-tabs__tab--selected-vertical {
    background: #37517e;
    color: #fff;
    box-shadow: 2px 2px 5px rgba(0,0,0,0.15);
    width: 100%
  }
  .footer {
    position: absolute;
    bottom: 0;
    width: 100%;
    height: 60px; /* Set the fixed height of the footer here */
    line-height: 60px; /* Vertically center the text there */
    background-color: #f5f5f5;
  }
  .container {
    width: auto;
    max-width: 680px;
    padding: 0 15px;
  }
`;
