"use strict";
/*
Copyright (c) 2021 VMware, Inc.
SPDX-License-Identifier: MIT

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/
Object.defineProperty(exports, "__esModule", { value: true });
exports.makeSpan = exports.ErrorDisplay = exports.ClosableCopyableContent = exports.zeroPad = exports.removeAllChildren = exports.HtmlString = exports.DataRangeUI = exports.percentString = exports.significantDigits = exports.px = exports.formatPositiveNumber = exports.SpecialChars = void 0;
var Direction;
(function (Direction) {
    Direction[Direction["Up"] = 0] = "Up";
    Direction[Direction["Down"] = 1] = "Down";
    Direction[Direction["None"] = 2] = "None";
})(Direction || (Direction = {}));
var SpecialChars = /** @class */ (function () {
    function SpecialChars() {
    }
    SpecialChars.upArrow = "▲";
    SpecialChars.downArrow = "▼";
    SpecialChars.clipboard = "\uD83D\uDCCB";
    SpecialChars.expand = "+";
    SpecialChars.shrink = "\u2501"; // heavy line
    SpecialChars.childAndNext = "├";
    SpecialChars.vertical = "│";
    SpecialChars.child = "└";
    return SpecialChars;
}());
exports.SpecialChars = SpecialChars;
/**
 * Converts a number to a more readable representation.
 * Negative numbers are shown as empty strings.
 */
function formatPositiveNumber(n) {
    if (n < 0)
        return "";
    return n.toLocaleString();
}
exports.formatPositiveNumber = formatPositiveNumber;
function px(dim) {
    if (dim === 0)
        return dim.toString();
    return dim.toString() + "px";
}
exports.px = px;
function createSvgElement(tag) {
    return document.createElementNS("http://www.w3.org/2000/svg", tag);
}
/**
 * Convert a number to an string by keeping only the most significant digits
 * and adding a suffix.  Similar to the previous function, but returns a pure string.
 */
function significantDigits(n) {
    var suffix = "";
    if (n === 0)
        return "0";
    var absn = Math.abs(n);
    if (absn > 1e12) {
        suffix = "T";
        n = n / 1e12;
    }
    else if (absn > 1e9) {
        suffix = "B";
        n = n / 1e9;
    }
    else if (absn > 1e6) {
        suffix = "M";
        n = n / 1e6;
    }
    else if (absn > 1e3) {
        suffix = "K";
        n = n / 1e3;
    }
    else if (absn < .001) {
        var expo = 0;
        while (n < 1) {
            n = n * 10;
            expo++;
        }
        suffix = " * 10e-" + expo;
    }
    if (absn > 1)
        n = Math.round(n * 100) / 100;
    else
        n = Math.round(n * 1000) / 1000;
    return String(n) + suffix;
}
exports.significantDigits = significantDigits;
function percentString(n) {
    n = Math.round(n * 1000) / 10;
    return significantDigits(n) + "%";
}
exports.percentString = percentString;
/**
 * A horizontal rectangle that displays a data range within an interval 0-max.
 */
var DataRangeUI = /** @class */ (function () {
    /**
     * @param position: Beginning of data range.
     * @param count:    Size of data range.
     * @param totalCount: Maximum value of interval.
     */
    function DataRangeUI(position, count, totalCount) {
        this.topLevel = createSvgElement("svg");
        this.topLevel.classList.add("dataRange");
        this.topLevel.setAttribute("width", px(DataRangeUI.width));
        this.topLevel.setAttribute("height", px(10));
        // If the range is no 0 but represents < 1 % of the total count, use 1% of the
        // bar's width, s.t. it is still visible.
        var w = count > 0 ? Math.max(0.01, count / totalCount) : count;
        var x = position / totalCount;
        if (x + w > 1)
            x = 1 - w;
        var g = createSvgElement("g");
        this.topLevel.appendChild(g);
        var title = createSvgElement("title");
        title.textContent = "(" + significantDigits(position) + " + " + significantDigits(count) + ") / " +
            significantDigits(totalCount) + "\n" +
            percentString(position / totalCount) + " + " + percentString(count / totalCount);
        g.appendChild(title);
        var rect1 = createSvgElement("rect");
        g.appendChild(rect1);
        rect1.setAttribute("x", x.toString() + "%");
        rect1.setAttribute("y", "0");
        rect1.setAttribute("fill", "black");
        rect1.setAttribute("width", w.toString() + "%");
        rect1.setAttribute("height", "1");
    }
    DataRangeUI.prototype.getDOMRepresentation = function () {
        return this.topLevel;
    };
    DataRangeUI.width = 80; // pixels
    return DataRangeUI;
}());
exports.DataRangeUI = DataRangeUI;
/**
 * HTML strings are strings that represent an HTML fragment.
 * They are usually assigned to innerHTML of a DOM object.
 * These strings should  be "safe": i.e., the HTML they contain should
 * be produced internally, and they should not come
 * from external sources, because they can cause DOM injection attacks.
 */
var HtmlString = /** @class */ (function () {
    function HtmlString(arg) {
        this.arg = arg;
        // Escape the argument string.
        var div = document.createElement("div");
        div.innerText = arg;
        this.safeValue = div.innerHTML;
    }
    HtmlString.prototype.appendSafeString = function (str) {
        this.safeValue += str;
    };
    HtmlString.prototype.append = function (message) {
        this.safeValue += message.safeValue;
    };
    HtmlString.prototype.prependSafeString = function (str) {
        this.safeValue = str + this.safeValue;
    };
    HtmlString.prototype.setInnerHtml = function (elem) {
        elem.innerHTML = this.safeValue;
    };
    HtmlString.prototype.clear = function () {
        this.safeValue = "";
    };
    HtmlString.prototype.getHTMLRepresentation = function () {
        var div = document.createElement("div");
        div.innerText = this.safeValue;
        return div;
    };
    return HtmlString;
}());
exports.HtmlString = HtmlString;
/**
 * Remove all children of an HTML DOM object..
 */
function removeAllChildren(h) {
    while (h.lastChild != null)
        h.removeChild(h.lastChild);
}
exports.removeAllChildren = removeAllChildren;
/**
 * convert a number to a string and prepend zeros if necessary to
 * bring the integer part to the specified number of digits
 */
function zeroPad(num, length) {
    var n = Math.abs(num);
    var zeros = Math.max(0, length - Math.floor(n).toString().length);
    var zeroString = Math.pow(10, zeros).toString().substr(1);
    if (num < 0) {
        zeroString = "-" + zeroString;
    }
    return zeroString + n;
}
exports.zeroPad = zeroPad;
/**
 * A div with two buttons: to close it, and to copy its content to the
 * clipboard.
 */
var ClosableCopyableContent = /** @class */ (function () {
    /**
     * Build a ClosableCopyableContent object.
     * @param onclose  Callback to invoke when the close button is pressed.
     */
    function ClosableCopyableContent(onclose) {
        var _this = this;
        this.onclose = onclose;
        this.topLevel = document.createElement("div");
        this.contents = document.createElement("div");
        var container = document.createElement("span");
        this.topLevel.appendChild(container);
        this.copyButton = document.createElement("span");
        this.copyButton.innerHTML = SpecialChars.clipboard;
        this.copyButton.title = "copy contents to clipboard";
        this.copyButton.style.display = "none";
        this.copyButton.classList.add("clickable");
        this.copyButton.onclick = function () { return _this.copyToClipboard(); };
        this.copyButton.style.cssFloat = "right";
        this.copyButton.style.zIndex = "10";
        this.clearButton = document.createElement("span");
        this.clearButton.classList.add("close");
        this.clearButton.innerHTML = "&times;";
        this.clearButton.title = "clear message";
        this.clearButton.style.display = "none";
        this.clearButton.onclick = function () { return _this.clear(); };
        this.clearButton.style.zIndex = "10";
        container.appendChild(this.clearButton);
        container.appendChild(this.copyButton);
        container.appendChild(this.contents);
    }
    ClosableCopyableContent.prototype.copyToClipboard = function () {
        navigator.clipboard.writeText(this.contents.innerText);
    };
    ClosableCopyableContent.prototype.getHTMLRepresentation = function () {
        return this.topLevel;
    };
    ClosableCopyableContent.prototype.showButtons = function () {
        this.clearButton.style.display = "block";
        this.copyButton.style.display = "block";
    };
    ClosableCopyableContent.prototype.setContent = function (message) {
        this.contents.innerText = message;
        this.showButtons();
    };
    ClosableCopyableContent.prototype.appendChild = function (elem) {
        this.contents.appendChild(elem);
        this.showButtons();
    };
    ClosableCopyableContent.prototype.hideButtons = function () {
        this.clearButton.style.display = "none";
        this.copyButton.style.display = "none";
    };
    ClosableCopyableContent.prototype.clear = function () {
        this.contents.textContent = "";
        this.hideButtons();
        this.onclose();
    };
    /**
     * css class to add to the content box.
     */
    ClosableCopyableContent.prototype.addContentClass = function (className) {
        this.contents.classList.add(className);
    };
    return ClosableCopyableContent;
}());
exports.ClosableCopyableContent = ClosableCopyableContent;
/**
 * This class is used to display error messages in the browser window.
 */
var ErrorDisplay = /** @class */ (function () {
    function ErrorDisplay() {
        this.content = new ClosableCopyableContent(function () { });
        this.content.addContentClass("console");
        this.content.addContentClass("error");
    }
    ErrorDisplay.prototype.getHTMLRepresentation = function () {
        return this.content.getHTMLRepresentation();
    };
    ErrorDisplay.prototype.reportError = function (message) {
        console.log("Error: " + message);
        this.content.setContent(message);
    };
    ErrorDisplay.prototype.clear = function () {
        this.content.clear();
    };
    return ErrorDisplay;
}());
exports.ErrorDisplay = ErrorDisplay;
/**
 * A class that knows how to display profile data.
 */
var ProfileTable = /** @class */ (function () {
    function ProfileTable(errorReporter, name, isCpu) {
        this.errorReporter = errorReporter;
        this.name = name;
        this.isCpu = isCpu;
        this.showCpuHistogram = true;
        this.showMemHistogram = false;
        this.total = 0;
        this.table = document.createElement("table");
        this.data = [];
        var thead = this.table.createTHead();
        var header = thead.insertRow();
        var columns;
        this.displayedColumns = [];
        if (isCpu)
            columns = ProfileTable.cpuColumns;
        else
            columns = ProfileTable.memoryColumns;
        for (var _i = 0, columns_1 = columns; _i < columns_1.length; _i++) {
            var c = columns_1[_i];
            if (c == "histogram") {
                if (isCpu && !this.showCpuHistogram)
                    continue;
                if (!isCpu && !this.showMemHistogram)
                    continue;
            }
            this.displayedColumns.push(c);
            var cell = header.insertCell();
            cell.textContent = ProfileTable.cellNames.get(c);
            cell.classList.add(c);
        }
        this.tbody = this.table.createTBody();
    }
    ProfileTable.prototype.getId = function (row) {
        if (row.opid == null)
            return null;
        return this.name.replace(/[^a-zA-Z0-9]/g, "_").toLowerCase() + "_" + row.opid.toString();
    };
    ProfileTable.prototype.addDataRow = function (indent, rowIndex, row, lastInSequence, histogramStart) {
        var _this = this;
        var trow = this.tbody.insertRow(rowIndex);
        // Fix undefined values
        var safeRow = {
            locations: row.locations === undefined ? [] : row.locations,
            descr: row.descr === undefined ? "" : row.descr,
            size: row.size === undefined ? -1 : row.size,
            cpu_us: row.cpu_us === undefined ? -1 : row.cpu_us,
            children: row.children === undefined ? [] : row.children,
            dd_op: row.dd_op === undefined ? "" : row.dd_op,
            invocations: row.invocations === undefined ? -1 : row.invocations,
            opid: row.opid === undefined ? -1 : row.opid,
            short_descr: row.short_descr === undefined ? "" : row.short_descr
        };
        var id = this.getId(safeRow);
        var _loop_1 = function (k) {
            var key = k;
            var value = safeRow[key];
            if (k === SpecialChars.expand || k === "histogram") {
                return "continue";
            }
            var htmlValue = void 0;
            if (k === "cpu_us" || k === "invocations" || k === "size") {
                htmlValue = new HtmlString(formatPositiveNumber(value));
                //htmlValue = significantDigitsHtml(value as number);
            }
            else {
                htmlValue = new HtmlString(value.toString());
            }
            var cell = trow.insertCell();
            cell.classList.add(k);
            if (k === "opid") {
                htmlValue.clear();
                var prefix = "";
                if (indent > 0) {
                    for (var i = 0; i < indent - 1; i++)
                        prefix += SpecialChars.vertical;
                    var end = lastInSequence ? SpecialChars.child : SpecialChars.childAndNext;
                    htmlValue = new HtmlString(prefix + end);
                }
            }
            else if (k === "ddlog_op") {
                var a = document.createElement("a");
                a.text = value.toString();
                cell.appendChild(a);
                return "continue";
            }
            else if (k === "short_descr") {
                var str_1 = value.toString();
                var direction = (safeRow.descr != "" || safeRow.locations.length > 0) ? Direction.Down : Direction.None;
                ProfileTable.makeDescriptionContents(cell, str_1, direction);
                if (direction != Direction.None) {
                    cell.classList.add("clickable");
                    cell.onclick = function () { return _this.showDescription(trow, cell, str_1, safeRow); };
                }
                return "continue";
            }
            htmlValue.setInnerHtml(cell);
            if (k === "opid") {
                // Add an adjacent cell for expanding the row children
                var cell_1 = trow.insertCell();
                var children_1 = safeRow.children;
                if (children_1.length > 0) {
                    cell_1.textContent = SpecialChars.expand;
                    cell_1.title = "Expand";
                    cell_1.classList.add("clickable");
                    cell_1.onclick = function () { return _this.expand(indent + 1, trow, cell_1, children_1, histogramStart); };
                }
                if (id != null) {
                    cell_1.id = id;
                    cell_1.title = id;
                }
            }
            if ((k === "size" && this_1.showMemHistogram) || (k == "cpu_us" && this_1.showCpuHistogram)) {
                // Add an adjacent cell for the histogram
                var cell_2 = trow.insertCell();
                var numberValue = value;
                var rangeUI = new DataRangeUI(histogramStart, numberValue, this_1.total);
                cell_2.appendChild(rangeUI.getDOMRepresentation());
            }
        };
        var this_1 = this;
        for (var _i = 0, _a = this.displayedColumns; _i < _a.length; _i++) {
            var k = _a[_i];
            _loop_1(k);
        }
    };
    /**
     * Add the description as a child to the expandCell
     * @param expandCell    Cell to add the description to.
     * @param htmlContents  HTML string containing the desired contents.
     * @param direction          If true add a down arrow, else an up arrow.
     */
    ProfileTable.makeDescriptionContents = function (expandCell, htmlContents, direction) {
        var contents = document.createElement("span");
        contents.innerHTML = htmlContents;
        if (direction == Direction.None) {
            removeAllChildren(expandCell);
            expandCell.appendChild(contents);
        }
        else {
            var down = direction == Direction.Down;
            var arrow = makeSpan((down ? SpecialChars.downArrow : SpecialChars.upArrow) + " ");
            arrow.title = down ? "Show source code" : "Hide source code";
            var span = document.createElement("span");
            span.appendChild(arrow);
            span.appendChild(contents);
            removeAllChildren(expandCell);
            expandCell.appendChild(span);
        }
    };
    ProfileTable.prototype.showDescription = function (tRow, expandCell, description, row) {
        var _this = this;
        var descr = description;
        ProfileTable.makeDescriptionContents(expandCell, descr, Direction.Up);
        var newRow = this.tbody.insertRow(tRow.rowIndex); // no +1, since tbody has one fewer rows than the table
        newRow.insertCell(); // unused.
        var cell = newRow.insertCell();
        cell.colSpan = this.displayedColumns.length - 1;
        var contents = new ClosableCopyableContent(function () { return _this.hideDescription(tRow, expandCell, descr, row); });
        cell.appendChild(contents.getHTMLRepresentation());
        if (row.descr != "") {
            var description_1 = makeSpan("");
            description_1.innerHTML = row.descr + "<br>";
            contents.appendChild(description_1);
            if (row.locations.length > 0)
                contents.appendChild(document.createElement("hr"));
        }
        expandCell.onclick = function () { return _this.hideDescription(tRow, expandCell, descr, row); };
        var _loop_2 = function (loc) {
            ScriptLoader.instance.loadScript(loc.file, function (data) {
                _this.appendSourceCode(data, loc, contents);
            }, this_2.errorReporter);
        };
        var this_2 = this;
        for (var _i = 0, _a = row.locations; _i < _a.length; _i++) {
            var loc = _a[_i];
            _loop_2(loc);
        }
    };
    ProfileTable.prototype.hideDescription = function (tRow, expandCell, description, row) {
        var _this = this;
        ProfileTable.makeDescriptionContents(expandCell, description, Direction.Down);
        this.table.deleteRow(tRow.rowIndex + 1);
        expandCell.onclick = function () { return _this.showDescription(tRow, expandCell, description, row); };
    };
    ProfileTable.prototype.appendSourceCode = function (contents, loc, contentHolder) {
        var snippet = document.createElement("div");
        snippet.classList.add("console");
        contentHolder.appendChild(snippet);
        contents.insertSnippet(snippet, loc, 1, 1, this.errorReporter);
    };
    ProfileTable.prototype.getDataSize = function (row) {
        if (row.cpu_us)
            return row.cpu_us;
        if (row.size)
            return row.size;
        return 0;
    };
    ProfileTable.prototype.expand = function (indent, tRow, expandCell, rows, histogramStart) {
        var _this = this;
        expandCell.textContent = SpecialChars.shrink;
        expandCell.title = "Shrink";
        var start = histogramStart;
        expandCell.onclick = function () { return _this.shrink(indent - 1, tRow, expandCell, rows, start); };
        var index = tRow.rowIndex;
        for (var rowIndex = 0; rowIndex < rows.length; rowIndex++) {
            var row = rows[rowIndex];
            this.addDataRow(indent, index++, row, rowIndex == rows.length - 1, histogramStart);
            histogramStart += this.getDataSize(row);
        }
    };
    ProfileTable.prototype.shrink = function (indent, tRow, expandCell, rows, histogramStart) {
        var _this = this;
        var index = tRow.rowIndex;
        expandCell.textContent = SpecialChars.expand;
        expandCell.title = "Expand";
        var start = histogramStart;
        expandCell.onclick = function () { return _this.expand(indent + 1, tRow, expandCell, rows, start); };
        while (true) {
            var row = this.table.rows[index + 1];
            if (row == null)
                break;
            if (row.cells.length < this.displayedColumns.length) {
                // This is a description row.
                this.table.deleteRow(index + 1);
                continue;
            }
            var text = row.cells[0].textContent;
            var c = text[indent];
            // Based on c we can tell whether this is one of our
            // children or a new parent.
            if (c == SpecialChars.child ||
                c == SpecialChars.childAndNext ||
                c == SpecialChars.vertical)
                this.table.deleteRow(index + 1);
            else
                break;
        }
    };
    ProfileTable.prototype.getHTMLRepresentation = function () {
        return this.table;
    };
    /**
     * Populate the view with the specified data.
     * Whatever used to be displayed is removed.
     */
    ProfileTable.prototype.setData = function (rows) {
        this.data = rows;
        this.table.removeChild(this.tbody);
        this.tbody = this.table.createTBody();
        for (var _i = 0, rows_1 = rows; _i < rows_1.length; _i++) {
            var row = rows_1[_i];
            this.total += this.getDataSize(row);
        }
        var histogramStart = 0;
        for (var rowIndex = 0; rowIndex < rows.length; rowIndex++) {
            var row = rows[rowIndex];
            this.addDataRow(0, -1, row, rowIndex == rows.length - 1, histogramStart);
            histogramStart += this.getDataSize(row);
        }
    };
    ProfileTable.prototype.findPathFromRow = function (r, id) {
        var thisId = this.getId(r);
        if (thisId == null)
            return null;
        if (thisId == id) {
            return [thisId];
        }
        if (r.children == null)
            return null;
        for (var _i = 0, _a = r.children; _i < _a.length; _i++) {
            var c = _a[_i];
            var p = this.findPathFromRow(c, id);
            if (p != null) {
                p.push(thisId);
                return p;
            }
        }
        return null;
    };
    /**
     * Find a path of rows that leads to the element with the specified id.
     */
    ProfileTable.prototype.findPath = function (toId) {
        for (var _i = 0, _a = this.data; _i < _a.length; _i++) {
            var r = _a[_i];
            var path = this.findPathFromRow(r, toId);
            if (path != null)
                return path;
        }
        return null;
    };
    ProfileTable.prototype.expandPath = function (path) {
        path.reverse();
        for (var _i = 0, path_1 = path; _i < path_1.length; _i++) {
            var p = path_1[_i];
            var elem = document.getElementById(p);
            elem.click();
            elem.parentElement.classList.add("highlight");
        }
        var last = document.getElementById(path[path.length - 1]);
        last.scrollIntoView();
    };
    /**
     * Find the node with the specified id and make sure it is visible.
     * If no such node exists, then return false;
     * @param id
     */
    ProfileTable.prototype.navigate = function (id) {
        var path = this.findPath(id);
        if (path == null)
            return false;
        this.expandPath(path);
        return true;
    };
    ProfileTable.cpuColumns = [
        "opid", SpecialChars.expand, "cpu_us", "histogram",
        "invocations", "short_descr", "dd_op"
    ];
    ProfileTable.memoryColumns = [
        "opid", SpecialChars.expand, "size", "histogram", "short_descr", "dd_op"
    ];
    // how to rename column names in the table heading
    ProfileTable.cellNames = new Map([
        ["opid", ""],
        [SpecialChars.expand, ""],
        ["cpu_us", "μs"],
        ["histogram", "histogram"],
        ["invocations", "calls"],
        ["size", "size"],
        ["short_descr", "description"],
        ["dd_op", "DD operator"]
    ]);
    return ProfileTable;
}());
/**
 * Returns a span element containing the specified text.
 * @param text       Text to insert in span.
 * @param highlight  If true the span has class highlight.
 */
function makeSpan(text, highlight) {
    if (highlight === void 0) { highlight = false; }
    var span = document.createElement("span");
    if (text != null)
        span.textContent = text;
    if (highlight)
        span.className = "highlight";
    return span;
}
exports.makeSpan = makeSpan;
/**
 * Keeps a source file parsed into lines.
 */
var SourceFile = /** @class */ (function () {
    function SourceFile(filename, data) {
        this.filename = filename;
        // number of lines to expand on each click
        this.expandLines = 10;
        if (data == null) {
            // This can happen on error.
            this.lines = [];
            return;
        }
        this.lines = data.split("\n");
    }
    /**
     * Return a snippet of the source file as indicated by the location.
     * @param parent make the snippet the only child of this element
     * @param loc source code location of snippet
     * @param before lines to show before snippet
     * @param after  lines to show after snippet
     * @param reporter used to report errors
     */
    SourceFile.prototype.insertSnippet = function (parent, loc, before, after, reporter) {
        var _this = this;
        removeAllChildren(parent);
        // Line and column numbers are 1-based
        var startLine = loc.pos[0] - 1;
        var endLine = loc.pos[2] - 1;
        var startColumn = loc.pos[1] - 1;
        var endColumn = loc.pos[3] - 1;
        var len = (endLine + 1).toString().length;
        if (startLine >= this.lines.length) {
            reporter.reportError("File " + this.filename + " does not have " + startLine +
                " lines, but only " + this.lines.length);
        }
        var fileRow = makeSpan(loc.file);
        fileRow.classList.add("filename");
        parent.appendChild(fileRow);
        parent.appendChild(document.createElement("br"));
        var lastLineDisplayed = endLine + 1 + after;
        if (endColumn == 0)
            // no data at all from last line, so show one fewer
            lastLineDisplayed--;
        var firstLineDisplayed = Math.max(startLine - before, 0);
        lastLineDisplayed = Math.min(lastLineDisplayed, this.lines.length);
        if (firstLineDisplayed > 0) {
            var expandUp = makeSpan(SpecialChars.upArrow);
            expandUp.title = "Show lines before";
            expandUp.classList.add("clickable");
            expandUp.onclick = function () { return _this.insertSnippet(parent, loc, before + _this.expandLines, after, reporter); };
            parent.appendChild(expandUp);
            parent.appendChild(document.createElement("br"));
        }
        for (var line = firstLineDisplayed; line < lastLineDisplayed; line++) {
            var l = this.lines[line];
            var lineno = zeroPad(line + 1, len);
            parent.appendChild(makeSpan(lineno + "| "));
            if (line == startLine) {
                parent.appendChild(makeSpan(l.substr(0, startColumn)));
                if (line == endLine) {
                    parent.appendChild(makeSpan(l.substr(startColumn, endColumn - startColumn), true));
                    parent.appendChild(makeSpan(l.substr(endColumn)));
                }
                else {
                    parent.appendChild(makeSpan(l.substr(startColumn), true));
                }
            }
            if (line != startLine && line != endLine) {
                parent.appendChild(makeSpan(l, line >= startLine && line <= endLine));
            }
            if (line == endLine && line != startLine) {
                parent.appendChild(makeSpan(l.substr(0, endColumn), true));
                parent.appendChild(makeSpan(l.substr(endColumn)));
            }
            parent.appendChild(document.createElement("br"));
        }
        if (lastLineDisplayed < this.lines.length) {
            var expandDown = makeSpan(SpecialChars.downArrow);
            expandDown.classList.add("clickable");
            expandDown.title = "Show lines after";
            expandDown.onclick = function () { return _this.insertSnippet(parent, loc, before, after + _this.expandLines, reporter); };
            parent.appendChild(expandDown);
            parent.appendChild(document.createElement("br"));
        }
    };
    return SourceFile;
}());
/**
 * This class manage loading JavaScript at runtime.
 * The assumption is that each of these scripts will insert a value
 * into a global variable called globalMap using a key that is the filename itself.
 */
var ScriptLoader = /** @class */ (function () {
    function ScriptLoader() {
        this.data = new Map();
    }
    /**
     * Load a js script file with this name.  The script is supposed to
     * insert a key-value pair into the global variable globalMap.
     * The key is the file name.  The value is a string that is parsed into a SourceFile.
     * @param filename   Filename to load.
     * @param onload     Callback to invoke when file is loaded.  The SourceFile
     *                   associated to the filename is passed to the callback.
     * @param reporter   Reporter invoked on error.
     */
    ScriptLoader.prototype.loadScript = function (filename, onload, reporter) {
        var _this = this;
        console.log("Attempting to load " + filename);
        var value = this.data.get(filename);
        if (value !== undefined) {
            onload(value);
            return;
        }
        var script = document.createElement("script");
        script.src = "./src/" + filename + ".js";
        script.onload = function () {
            var value = getGlobalMapValue(filename);
            var source = new SourceFile(filename, value);
            _this.data.set(filename, source);
            if (value == null) {
                reporter.reportError("Loading file " + filename + " did not produce the expected result");
            }
            else {
                onload(source);
            }
        };
        document.head.append(script);
    };
    ScriptLoader.instance = new ScriptLoader();
    return ScriptLoader;
}());
/**
 * Main UI to display profile information.
 */
var ProfileUI = /** @class */ (function () {
    function ProfileUI() {
        this.topLevel = document.createElement("div");
        this.errorReporter = new ErrorDisplay();
        this.topLevel.appendChild(this.errorReporter.getHTMLRepresentation());
        this.profiles = [];
    }
    /**
     * Returns the html representation of the UI.
     */
    ProfileUI.prototype.getHTMLRepresentation = function () {
        return this.topLevel;
    };
    ProfileUI.prototype.addProfile = function (profile) {
        var h1 = document.createElement("h1");
        h1.textContent = profile.name;
        this.topLevel.appendChild(h1);
        var profileTable = new ProfileTable(this.errorReporter, profile.name, profile.type.toLowerCase() === "cpu");
        this.topLevel.appendChild(profileTable.getHTMLRepresentation());
        this.profiles.push(profileTable);
        profileTable.setData(profile.records);
    };
    ProfileUI.prototype.setData = function (profiles) {
        for (var _i = 0, profiles_1 = profiles; _i < profiles_1.length; _i++) {
            var profile = profiles_1[_i];
            this.addProfile(profile);
        }
    };
    /**
     * Find the element with the specified id and make sure it is visible.
     * @param id Element id to search for.
     */
    ProfileUI.prototype.navigate = function (id) {
        id = id.replace("#", "");
        for (var _i = 0, _a = this.profiles; _i < _a.length; _i++) {
            var p = _a[_i];
            if (p.navigate(id))
                break;
        }
    };
    return ProfileUI;
}());
/**
 * Main function exported by this module: instantiates the user interface.
 */
function createUI(profiles) {
    var top = document.getElementById("top");
    if (top == null) {
        console.log("Could not find 'top' element in document");
        return;
    }
    var ui = new ProfileUI();
    top.appendChild(ui.getHTMLRepresentation());
    ui.setData(profiles);
    ui.navigate(window.location.hash);
}
exports.default = createUI;
//# sourceMappingURL=ui.js.map