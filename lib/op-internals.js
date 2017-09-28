const _ = require('lodash');

function addExpressionAttributes(request, condition) {
  const expressionAttributeNames = _.merge({}, condition.attributeNames, request.ExpressionAttributeNames);
  const expressionAttributeValues = _.merge({}, condition.attributeValues, request.ExpressionAttributeValues);

  if (!_.isEmpty(expressionAttributeNames)) {
    request.ExpressionAttributeNames = expressionAttributeNames;
  }

  if (!_.isEmpty(expressionAttributeValues)) {
    request.ExpressionAttributeValues = expressionAttributeValues;
  }
}

function updateFilterExpression(request, condition) {
  if (_.isString(request.FilterExpression)) {
    request.FilterExpression = `${request.FilterExpression} AND (${condition.statement})`;
  } else {
    request.FilterExpression = `(${condition.statement})`;
  }
}

module.exports = {
  addExpressionAttributes,
  updateFilterExpression
};
