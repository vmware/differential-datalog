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

/**
 * Interface implemented by TypeScript objects that have an HTML rendering.
 * This returns the root of the HTML rendering.
 */
export interface IHtmlElement {
    getHTMLRepresentation(): HTMLElement;
}

export interface IAppendChild {
    appendChild(elem: HTMLElement): void;
}

export interface ErrorReporter {
    reportError(message: string): void;
}

export class SpecialChars {
    public static approx = "\u2248";
    public static upArrow = "▲";
    public static downArrow = "▼";
    public static ellipsis = "…";
    public static downArrowHtml = "&dArr;";
    public static upArrowHtml = "&uArr;";
    public static leftArrowHtml = "&lArr;";
    public static rightArrowHtml = "&rArr;";
    public static epsilon = "\u03B5";
    public static enDash = "&ndash;";
    public static scissors = "\u2702";
    public static expand = "+";
    public static shrink = "\u2501"; // heavy line
    public static childAndNext = "├";
    public static vertical = "│";
    public static child = "└";
    public static childPrefix = "\u2002"; // en space
}

/**
 * HTML strings are strings that represent an HTML fragment.
 * They are usually assigned to innerHTML of a DOM object.
 * These strings should  be "safe": i.e., the HTML they contain should
 * be produced internally, and they should not come
 * from external sources, because they can cause DOM injection attacks.
 */
export class HtmlString implements IHtmlElement {
    private safeValue: string;

    constructor(private arg: string) {
        // Escape the argument string.
        const div = document.createElement("div");
        div.innerText = arg;
        this.safeValue = div.innerHTML;
    }

    public appendSafeString(str: string): void {
        this.safeValue += str;
    }

    public append(message: HtmlString): void {
        this.safeValue += message.safeValue;
    }

    public prependSafeString(str: string): void {
        this.safeValue = str + this.safeValue;
    }

    public setInnerHtml(elem: HTMLElement): void {
        elem.innerHTML = this.safeValue;
    }

    public clear(): void {
        this.safeValue = "";
    }

    public getHTMLRepresentation(): HTMLElement {
        const div = document.createElement("div");
        div.innerText = this.safeValue;
        return div;
    }
}

/**
 * Remove all children of an HTML DOM object..
 */
export function removeAllChildren(h: HTMLElement): void {
    while (h.lastChild != null)
        h.removeChild(h.lastChild);
}

/**
 * Display only the significant digits of a number; returns an HtmlString.
 * @param n  number to display for human consumption.
 */
export function significantDigitsHtml(n: number): HtmlString {
    let suffix = "";
    if (n === 0)
        return new HtmlString("0");
    const absn = Math.abs(n);
    if (absn > 1e12) {
        suffix = "T";
        n = n / 1e12;
    } else if (absn > 1e9) {
        suffix = "B";
        n = n / 1e9;
    } else if (absn > 1e6) {
        suffix = "M";
        n = n / 1e6;
    } else if (absn > 1e3) {
        suffix = "K";
        n = n / 1e3;
    } else if (absn < .001) {
        let expo = 0;
        while (n < .1) {
            n = n * 10;
            expo++;
        }
        suffix = "&times;10<sup>-" + expo + "</sup>";
    }
    if (absn > 1)
        n = Math.round(n * 100) / 100;
    else
        n = Math.round(n * 1000) / 1000;
    const result = new HtmlString(String(n));
    result.appendSafeString(suffix);
    return result;
}

/**
 * convert a number to a string and prepend zeros if necessary to
 * bring the integer part to the specified number of digits
 */
export function zeroPad(num: number, length: number): string {
    const n = Math.abs(num);
    const zeros = Math.max(0, length - Math.floor(n).toString().length );
    let zeroString = Math.pow(10, zeros).toString().substr(1);
    if (num < 0) {
        zeroString = "-" + zeroString;
    }

    return zeroString + n;
}

export function repeat(c: string, count: number): string {
    let result = "";
    for (let i = 0; i < count; i++)
        result += c;
    return result;
}

/**
 * A location inside source code.
 * Used for parsing the JSON input.
 */
interface Location {
    file: string,
    pos: [number, number, number, number]
}

/**
 * A row in a profile dataset.
 */
interface ProfileRow {
    opid: number,
    cpu_us: number,
    invocations?: number;
    size?: number;
    locations: Location[],
    descr: string,
    short_descr: string,
    dd_op: string,
    doc: string,
    children?: ProfileRow[]
}

/**
 * A div with two buttons: to close it, and to copy its content to the
 * clipboard.
 */
export class ClosableCopyableContent implements IHtmlElement, IAppendChild {
    protected topLevel: HTMLElement;
    protected contents: HTMLDivElement;
    protected clearButton: HTMLElement;
    protected copyButton: HTMLElement;

    /**
     * Build a ClosableCopyableContent object.
     * @param onclose  Callback to invoke when the close button is pressed.
     */
    constructor(protected onclose: () => void) {
        this.topLevel = document.createElement("div");
        this.contents = document.createElement("div");
        const container = document.createElement("span");
        this.topLevel.appendChild(container);

        this.copyButton = document.createElement("span");
        this.copyButton.innerHTML = SpecialChars.scissors;
        this.copyButton.title = "copy contents to clipboard";
        this.copyButton.style.display = "none";
        this.copyButton.classList.add("clickable");
        this.copyButton.onclick = () => this.copyToClipboard();
        this.copyButton.style.cssFloat = "right";
        this.copyButton.style.zIndex = "10";

        this.clearButton = document.createElement("span");
        this.clearButton.classList.add("close");
        this.clearButton.innerHTML = "&times;";
        this.clearButton.title = "clear message";
        this.clearButton.style.display = "none";
        this.clearButton.onclick = () => this.clear();
        this.clearButton.style.zIndex = "10";

        container.appendChild(this.clearButton);
        container.appendChild(this.copyButton);
        container.appendChild(this.contents);
    }

    public copyToClipboard(): void {
        navigator.clipboard.writeText(this.contents.innerText);
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    protected showButtons(): void {
        this.clearButton.style.display = "block";
        this.copyButton.style.display = "block";
    }

    public setContent(message: string): void {
        this.contents.innerText = message;
        this.showButtons();
    }

    public appendChild(elem: HTMLElement): void {
        this.contents.appendChild(elem);
        this.showButtons();
    }

    protected hideButtons(): void {
        this.clearButton.style.display = "none";
        this.copyButton.style.display = "none";
    }

    public clear(): void {
        this.contents.textContent = "";
        this.hideButtons();
        this.onclose();
    }

    /**
     * css class to add to the content box.
     */
    public addContentClass(className: string): void {
        this.contents.classList.add(className);
    }
}

/**
 * This class is used to display error messages in the browser window.
 */
export class ErrorDisplay implements ErrorReporter, IHtmlElement {
    protected content: ClosableCopyableContent;

    constructor() {
        this.content = new ClosableCopyableContent(() => {});
        this.content.addContentClass("console");
        this.content.addContentClass("error");
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.content.getHTMLRepresentation();
    }

    public reportError(message: string): void {
        console.log("Error: " + message);
        this.content.setContent(message);
    }

    public clear(): void {
        this.content.clear();
    }
}

/**
 * A class that knows how to display profile data.
 */
class ProfileTable implements IHtmlElement {
    protected tbody: HTMLTableSectionElement;
    protected table: HTMLTableElement;

    protected static readonly cpuColumns: string[] = [
        "opid", SpecialChars.expand, "cpu_us", "invocations",
        "short_descr", "dd_op" ];
    protected static readonly memoryColumns: string[] = [
        "opid", SpecialChars.expand, "size",
        "short_descr", "dd_op" ];
    // how to rename column names in the table heading
    protected static readonly cellNames = new Map<string, string>([
        ["opid", ""],
        [SpecialChars.expand, SpecialChars.expand],
        ["cpu_us", "μs"],
        ["invocations", "calls"],
        ["size", "size"],
        ["short_descr", "description"],
        ["dd_op", "operation"]
    ]);
    protected static readonly baseDocUrl = "https://github.com/vmware/differential-datalog/wiki/profiler_help#";
    protected displayedColumns: string[];

    constructor(protected errorReporter: ErrorReporter, protected isCpu: boolean) {
        this.table = document.createElement("table");
        const thead = this.table.createTHead();
        const header = thead.insertRow();
        if (isCpu)
            this.displayedColumns = ProfileTable.cpuColumns;
        else
            this.displayedColumns = ProfileTable.memoryColumns;
        for (let c of this.displayedColumns) {
            const cell = header.insertCell();
            cell.textContent = ProfileTable.cellNames.get(c)!;
            cell.classList.add(c);
        }
        this.tbody = this.table.createTBody();
    }

    protected addDataRow(indent: number, rowIndex: number, row: ProfileRow, lastInSequence: boolean): void {
        const trow = this.tbody.insertRow(rowIndex);
        for (let k of this.displayedColumns) {
            let key = k as keyof ProfileRow;
            let value = row[key];
            if (k === SpecialChars.expand) {
                // The second column is not really a property
                continue;
            }
            if (value === undefined && k != "invocations" && k != "size") {
                this.errorReporter.reportError("Missing value for column " + k);
            }
            let htmlValue: HtmlString;
            if (k === "cpu_us") {
                if (value === undefined) {
                    value = "";
                    htmlValue = new HtmlString("");
                } else {
                    htmlValue = significantDigitsHtml(value as number);
                }
            } else {
                if (value === undefined)
                    value = "";
                htmlValue = new HtmlString(value.toString());
            }
            const cell = trow.insertCell();
            cell.classList.add(k);
            if (k === "opid") {
                htmlValue.clear();
                let prefix = "";
                if (indent > 0) {
                    for (let i = 0; i < indent - 1; i++)
                        prefix += SpecialChars.vertical;
                    const end = lastInSequence ? SpecialChars.child : SpecialChars.childAndNext;
                    htmlValue = new HtmlString(prefix + end);
                }
            } else if (k === "ddlog_op") {
                const section = row["doc"];
                const a = document.createElement("a");
                a.href = ProfileTable.baseDocUrl + section;
                a.text = value.toString();
                cell.appendChild(a);
                continue;
            } else if (k === "short_descr") {
                const str = value.toString() ?? "";
                ProfileTable.makeDescriptionContents(cell, str, true);
                cell.classList.add("clickable");
                cell.onclick = () => this.showDescription(trow, cell, str, row);
                continue;
            }
            htmlValue.setInnerHtml(cell);
            if (k === "opid") {
                // Add an adjacent cell for expanding the row children
                const cell = trow.insertCell();
                cell.classList.add("noborder");
                const children = row.children != null ? row.children : [];
                if (children.length > 0) {
                    cell.textContent = SpecialChars.expand;
                    cell.title = "Expand";
                    cell.classList.add("clickable");
                    cell.onclick = () => this.expand(indent + 1, trow, cell, children);
                }
            }
        }
    }

    /**
     * Add the description as a child to the expandCell
     * @param expandCell    Cell to add the description to.
     * @param htmlContents  HTML string containing the desired contents.
     * @param down          If true add a down arrow, else an up arrow.
     */
    static makeDescriptionContents(expandCell: HTMLElement, htmlContents: string, down: boolean): void {
        const arrow = makeSpan((down ? SpecialChars.downArrow : SpecialChars.upArrow) + " ");
        arrow.title = down ? "Show source code" : "Hide source code";
        const contents = document.createElement("span");
        contents.innerHTML = htmlContents;
        const span = document.createElement("span");
        span.appendChild(arrow);
        span.appendChild(contents);
        removeAllChildren(expandCell);
        expandCell.appendChild(span);
    }

    protected showDescription(tRow: HTMLTableRowElement, expandCell: HTMLTableCellElement,
                              description: string, row: ProfileRow): void {
        ProfileTable.makeDescriptionContents(expandCell, description, false);
        const newRow = this.tbody.insertRow(tRow.rowIndex); // no +1, since tbody has one fewer rows than the table
        newRow.insertCell();  // unused.
        const cell = newRow.insertCell();
        cell.colSpan = this.displayedColumns.length - 1;
        const contents = new ClosableCopyableContent(
            () => this.hideDescription(tRow, expandCell, description, row));
        cell.appendChild(contents.getHTMLRepresentation());
        contents.addContentClass("console");
        expandCell.onclick = () => this.hideDescription(tRow, expandCell, description, row);
        for (let loc of row.locations) {
            ScriptLoader.instance.loadScript(loc.file, (data: SourceFile) => {
                this.appendSourceCode(data, loc, contents);
            }, this.errorReporter)
        }
    }

    protected hideDescription(tRow: HTMLTableRowElement, expandCell: HTMLTableCellElement,
                              description: string, row: ProfileRow): void {
        ProfileTable.makeDescriptionContents(expandCell, description, true);
        this.table.deleteRow(tRow.rowIndex + 1);
        expandCell.onclick = () => this.showDescription(tRow, expandCell, description, row);
    }

    protected appendSourceCode(contents: SourceFile, loc: Location, contentHolder: IAppendChild): void {
        const snippet = contents.getSnippet(loc, this.errorReporter);
        contentHolder.appendChild(snippet);
    }

    protected expand(indent: number, tRow: HTMLTableRowElement, expandCell: HTMLTableCellElement, rows: ProfileRow[]): void {
        expandCell.textContent = SpecialChars.shrink;
        expandCell.title = "Shrink";
        expandCell.onclick = () => this.shrink(indent - 1, tRow, expandCell, rows);
        let index = tRow.rowIndex;
        for (let rowIndex = 0; rowIndex < rows.length; rowIndex++) {
            const row = rows[rowIndex];
            this.addDataRow(indent, index++, row, rowIndex == rows.length - 1);
        }
    }

    protected shrink(indent: number, tRow: HTMLTableRowElement, expandCell: HTMLTableCellElement, rows: ProfileRow[]): void {
        let index = tRow.rowIndex;
        expandCell.textContent = SpecialChars.expand;
        expandCell.title = "Expand";
        expandCell.onclick = () => this.expand(indent + 1, tRow, expandCell, rows);
        while (true) {
            const row = this.table.rows[index + 1];
            if (row == null)
                break;
            if (row.cells.length < this.displayedColumns.length) {
                // This is a description row.
                this.table.deleteRow(index + 1);
                continue;
            }
            const text = row.cells[0].textContent;
            const c = text![indent];
            // Based on c we can tell whether this is one of our
            // children or a new parent.
            if (c == SpecialChars.child ||
                c == SpecialChars.childAndNext ||
                c == SpecialChars.vertical)
                this.table.deleteRow(index + 1);
            else
                break;
        }
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.table;
    }

    /**
     * Populate the view with the specified data.
     * Whatever used to be displayed is removed.
     */
    public setData(rows: ProfileRow[]): void {
        this.table.removeChild(this.tbody);
        this.tbody = this.table.createTBody();
        for (let rowIndex = 0; rowIndex < rows.length; rowIndex++) {
            const row = rows[rowIndex];
            this.addDataRow(0, -1, row, rowIndex == rows.length - 1);
        }
    }
}

/**
 * Returns a span element containing the specified text.
 * @param text       Text to insert in span.
 * @param highlight  If true the span has class highlight.
 */
export function makeSpan(text: string | null, highlight: boolean = false): HTMLElement {
    const span = document.createElement("span");
    if (text != null)
        span.textContent = text;
    if (highlight)
        span.className = "highlight";
    return span;
}


// This function is defined in JavaScript
declare function getGlobalMapValue(key: string): string | null;

/**
 * Keeps a source file parsed into lines.
 */
class SourceFile {
    protected lines: string[];

    constructor(protected filename: string, data: string | null) {
        if (data == null) {
            // This can happen on error.
            this.lines = [];
            return;
        }
        this.lines = data.split("\n");
    }

    /**
     * Return a snippet of the source file as indicated by the location.
     */
    public getSnippet(loc: Location, reporter: ErrorReporter): HTMLElement {
        const result = makeSpan("");
        // Line and column numbers are 1-based
        const startLine = loc.pos[0] - 1;
        const endLine = loc.pos[2] - 1;
        const startColumn = loc.pos[1] - 1;
        const endColumn = loc.pos[3] - 1;
        const len = (endLine + 1).toString().length;
        if (startLine >= this.lines.length) {
            reporter.reportError("File " + this.filename + " does not have " + startLine +
                " lines, but only " + this.lines.length);
            return result;
        }
        result.appendChild(makeSpan(loc.file));
        result.appendChild(document.createElement("br"));
        let lastLineDisplayed = endLine + 2;
        if (endColumn == 0)
            // no data at all from last line, so show one fewer
            lastLineDisplayed--;
        for (let line = Math.max(startLine - 1, 0);
            line < Math.min(lastLineDisplayed, this.lines.length); line++) {
            let l = this.lines[line];
            const lineno = zeroPad(line + 1, len);
            result.appendChild(makeSpan(lineno + "| "));
            if (line == startLine) {
                result.appendChild(makeSpan(l.substr(0, startColumn)));
                if (line == endLine) {
                    result.appendChild(makeSpan(l.substr(startColumn, endColumn - startColumn), true));
                    result.appendChild(makeSpan(l.substr(endColumn)));
                } else {
                    result.appendChild(makeSpan(l.substr(startColumn), true));
                }
            }
            if (line != startLine && line != endLine) {
                result.appendChild(makeSpan(l, line >= startLine && line <= endLine));
            }
            if (line == endLine && line != startLine) {
                result.appendChild(makeSpan(l.substr(0, endColumn), true));
                result.appendChild(makeSpan(l.substr(endColumn)));
            }
            result.appendChild( document.createElement("br"));
        }
        return result;
    }
}

/**
 * This class manage loading JavaScript at runtime.
 * The assumption is that each of these scripts will insert a value
 * into a global variable called globalMap using a key that is the filename itself.
 */
class ScriptLoader {
    public static instance: ScriptLoader = new ScriptLoader();
    protected data: Map<string, SourceFile>;

    private constructor() {
        this.data = new Map<string, SourceFile>();
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
    public loadScript(filename: string, onload: (source: SourceFile) => void, reporter: ErrorReporter): void {
        console.log("Attempting to load " + filename);
        const value = this.data.get(filename);
        if (value !== undefined) {
            onload(value);
            return;
        }
        const script = document.createElement("script");
        script.src = "./src/" + filename + ".js";
        script.onload = () => {
            const value = getGlobalMapValue(filename);
            const source = new SourceFile(filename, value);
            this.data.set(filename, source);
            if (value == null) {
                reporter.reportError("Loading file " + filename + " did not produce the expected result");
            } else {
                onload(source);
            }
        };
        document.head.append(script);
    }
}

/**
 * Main UI to display profile information.
 */
class ProfileUI implements IHtmlElement {
    protected topLevel: HTMLElement;
    protected errorReporter: ErrorDisplay;
    protected profileTable: ProfileTable;

    constructor() {
        const dropBox = document.getElementById("dropbox");
        dropBox!.addEventListener("dragenter", (e) => ProfileUI.defaultEvent(e), false);
        dropBox!.addEventListener("dragover", (e) => ProfileUI.defaultEvent(e), false );
        dropBox!.addEventListener("drop", (e) => this.fileSelected(e), false);
        this.topLevel = document.createElement("div");
        this.errorReporter = new ErrorDisplay();
        this.topLevel.appendChild(this.errorReporter.getHTMLRepresentation());
        this.profileTable = new ProfileTable(this.errorReporter, true);
        this.topLevel.appendChild(this.profileTable.getHTMLRepresentation())
    }

    protected reportError(message: string): void {
        this.errorReporter.reportError(message);
    }

    protected fileSelected(e: DragEvent): void {
        ProfileUI.defaultEvent(e);
        const dt = e.dataTransfer;
        if (dt === null)
            return;
        this.loadFile(dt.files);
    }

    protected loadFile(f: FileList): void {
        this.errorReporter.clear();
        for (let i = 0; i < f.length; i++) {
            const file = f[i];
            this.errorReporter.reportError("Reading " + file.name);
            const reader = new FileReader();
            reader.onload = (ev: ProgressEvent<FileReader>) => {
                if (ev.target == null)
                    return;
                const data = ev.target.result;
                this.dataLoaded(data as string);
            };
            reader.readAsText(file);
        }
    }

    protected dataLoaded(data: string | null): void {
        if (data == null) {
            this.reportError("Could not load data");
            return;
        }
        try {
            const rows = JSON.parse(data) as ProfileRow[];
            this.errorReporter.clear();
            this.profileTable.setData(rows);
        } catch (e) {
            this.errorReporter.reportError((e as Error).message);
        }
    }

    protected static defaultEvent(e: Event): void {
        e.stopPropagation();
        e.preventDefault();
    }

    /**
     * Returns the html representation of the UI.
     */
    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }
}

/**
 * Main function exported by this module: instantiates the user interface.
 */
export default function createUI(): void {
    const top = document.getElementById("top");
    if (top == null) {
        console.log("Could not find 'top' element in document");
        return;
    }
    const ui = new ProfileUI();
    top.appendChild(ui.getHTMLRepresentation());
}

export {
    createUI
};