
module.exports = function enablePagination(opBuilder) {
  opBuilder.canContinue = function () {
    return !!this.position();
  };

  opBuilder.position = function () {
    return this.request.ExclusiveStartKey;
  };

  opBuilder.continue = function (callback) {
    if (!this.canContinue()) {
      return setImmediate(callback, new Error('cannot continue'));
    }

    this.exec(callback);
  };
};
