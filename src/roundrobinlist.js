"use strict";

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

/**
 * Takes a list and cycles through the elements in the list repeatedly and
 * in-order. Adding and removing to the list does not perturb the order.
 *
 * Usage:
 *   const list = RoundRobinList([1, 2, 3]);
 *   list.next() ==> [1]
 *   list.next(2) ==> [2, 3]
 *   list.next(2) ==> [1, 2]
 *   list.add(5) ==> 5
 *   list.next(2) ==> [3, 5]
 */
var RoundRobinList = function () {
  /**
   * Instantiate a new RoundRobinList.
   *
   * @param  {Array} list
   */
  function RoundRobinList(list) {
    _classCallCheck(this, RoundRobinList);

    this.list = list.slice();
    this.index = 0;
  }

  /**
   * Returns the length of the list.
   *
   * @return {Number}
   */


  _createClass(RoundRobinList, [{
    key: "length",
    value: function length() {
      return this.list.length;
    }

    /**
     * Add an item to the list.
     *
     * @param {*} item
     * @return {*} The item added.
     */

  }, {
    key: "add",
    value: function add(item) {
      return this.list.push(item);
    }

    /**
     * Remove an item from the list.
     *
     * @param  {*} item
     * @return {Array|undefined}
     */

  }, {
    key: "remove",
    value: function remove(item) {
      var itemIndex = this.list.indexOf(item);
      if (itemIndex === -1) return;

      if (this.index > itemIndex) {
        this.index -= 1;
      }

      return this.list.splice(itemIndex, 1);
    }

    /**
     * Get the next items in the list, round robin style.
     *
     * @param  {Number}   [count=1]
     * @return {Array}
     */

  }, {
    key: "next",
    value: function next() {
      var count = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : 1;
      var index = this.index;

      this.index = (this.index + count) % this.list.length;
      return this.list.slice(index, index + count);
    }
  }]);

  return RoundRobinList;
}();

module.exports = RoundRobinList;