"use strict";

ckan.module('datastore_resource', function ($, _) {
  return {
    initialize: function () {
      console.log("I've been initialized for element: ", this.el);
    }
  };
});