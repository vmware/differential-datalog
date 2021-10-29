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
    ddlog_op: string,
    dd_op: string,
    doc: string,
    children?: ProfileRow[]
}

/**
 * Interface implemented by TypeScript objects that have an HTML rendering.
 * This returns the root of the HTML rendering.
 */
export interface IHtmlElement {
    getHTMLRepresentation(): HTMLElement;
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

interface ErrorReporter {
    reportError(message: string): void;
}

interface IAppendChild {
    appendChild(elem: HTMLElement): void;
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

    public setContent(message: string): void {
        this.contents.innerText = message;
        this.clearButton.style.display = "block";
        this.copyButton.style.display = "block";
    }

    public appendChild(elem: HTMLElement): void {
        this.contents.appendChild(elem);
    }

    public clear(): void {
        this.contents.textContent = "";
        this.clearButton.style.display = "none";
        this.copyButton.style.display = "none";
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
 * HTML strings are strings that represent an HTML fragment.
 * They are usually assigned to innerHTML of a DOM object.
 * These strings should  be "safe": i.e., the HTML they contain should
 * be produced internally, and they should not come
 * from external sources, because they can cause DOM injection attacks.
 */
export class HtmlString {
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
 * A class that knows how to display profile data.
 */
class ProfileTable implements IHtmlElement {
    protected tbody: HTMLTableSectionElement;
    protected table: HTMLTableElement;
    protected static readonly cells: string[] = [ "opid", SpecialChars.expand, "cpu_us", "invocations", "size",
        "short_descr", "ddlog_op", "dd_op" ];
    // how to rename column names in the table heading
    protected static readonly cellNames = new Map<string, string>([
        ["opid", ""],
        [SpecialChars.expand, SpecialChars.expand],
        ["cpu_us", "μs"],
        ["invocations", "calls"],
        ["size", "size"],
        ["short_descr", "description"],
        ["ddlog_op", "operation"],
        ["dd_op", "differential op"]
    ]);
    protected static readonly baseDocUrl = "https://github.com/vmware/differential-datalog/wiki/profiler_help#";

    constructor(protected errorReporter: ErrorReporter) {
        this.table = document.createElement("table");
        const thead = this.table.createTHead();
        const header = thead.insertRow();
        for (let c of ProfileTable.cells) {
            const cell = header.insertCell();
            cell.textContent = ProfileTable.cellNames.get(c)!;
            cell.classList.add(c);
        }
        this.tbody = this.table.createTBody();
    }

    protected addDataRow(indent: number, rowIndex: number, row: ProfileRow, lastInSequence: boolean): void {
        const trow = this.tbody.insertRow(rowIndex);
        for (let k of ProfileTable.cells) {
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
                htmlValue.prependSafeString(SpecialChars.downArrow + " ");
                cell.classList.add("clickable");
                let descr = row["descr"];
                if (descr == null || descr == "")
                    descr = "<no description provided>";
                cell.onclick = () => this.showDescription(trow, cell, descr, row);
            }
            htmlValue.setInnerHtml(cell);
            if (k === "opid") {
                // Add an adjacent cell for expanding the row children
                const cell = trow.insertCell();
                cell.classList.add("noborder");
                const children = row.children != null ? row.children : [];
                if (children.length > 0) {
                    cell.textContent = SpecialChars.expand;
                    cell.classList.add("clickable");
                    cell.onclick = () => this.expand(indent + 1, trow, cell, children);
                }
            }
        }
    }

    protected showDescription(tRow: HTMLTableRowElement, expandCell: HTMLTableCellElement,
                              description: string, row: ProfileRow): void {
        const newRow = this.tbody.insertRow(tRow.rowIndex); // no +1, since tbody has one fewer rows than the table
        newRow.insertCell();  // unused.
        const cell = newRow.insertCell();
        cell.colSpan = ProfileTable.cells.length - 1;
        const contents = new ClosableCopyableContent(
            () => this.hideDescription(tRow, expandCell, description, row));
        cell.appendChild(contents.getHTMLRepresentation());
        contents.addContentClass("description");
        contents.setContent(description);
        expandCell.onclick = () => this.hideDescription(tRow, expandCell, description, row);
        for (let loc of row.locations) {
            ScriptLoader.instance.loadScript(loc.file, (data: SourceFile) => {
                this.appendLocationInformation(data, loc, contents);
            }, this.errorReporter)
        }
        //cell.classList.add("clickable");
        //cell.onclick = () => this.hideDescription(tRow, expandCell, description);
    }

    protected appendLocationInformation(contents: SourceFile, loc: Location, contentHolder: IAppendChild): void {
        const snippet = contents.getSnippet(loc, this.errorReporter);
        if (snippet != null) {
            const contents = new ClosableCopyableContent(() => {});
            contents.addContentClass("console");
            contents.setContent(snippet);
            contentHolder.appendChild(contents.getHTMLRepresentation());
        }
    }

    protected hideDescription(tRow: HTMLTableRowElement, expandCell: HTMLTableCellElement,
                              description: string, row: ProfileRow): void {
        this.table.deleteRow(tRow.rowIndex + 1);
        expandCell.onclick = () => this.showDescription(tRow, expandCell, description, row);
    }

    protected expand(indent: number, tRow: HTMLTableRowElement, expandCell: HTMLTableCellElement, rows: ProfileRow[]): void {
        expandCell.textContent = SpecialChars.shrink;
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
        expandCell.onclick = () => this.expand(indent + 1, tRow, expandCell, rows);
        while (true) {
            const row = this.table.rows[index + 1];
            if (row == null)
                break;
            if (row.cells.length < ProfileTable.cells.length) {
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

function repeat(c: string, count: number): string {
    let result = "";
    for (let i = 0; i < count; i++)
        result += c;
    return result;
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
    public getSnippet(loc: Location, reporter: ErrorReporter): string {
        const startLine = loc.pos[0];
        const endLine = loc.pos[2];
        const startColumn = loc.pos[1] + 1;
        const endColumn = loc.pos[3] + 1;
        let result = "";
        for (let line = startLine; line <= endLine; line++) {
            if (line >= this.lines.length) {
                reporter.reportError("File " + this.filename + " does not have " + line +
                    " lines, but only " + this.lines.length);
                return result;
            }
            let l = this.lines[line];
            if (line == startLine) {
                l = repeat(" ", startColumn) + l.substr(startColumn);
            }
            if (line == endLine) {
                l = l.substr(0, endColumn);
            }
            result += l + "\n";
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
        if (value != null) {
            onload(value);
            return;
        }
        const script = document.createElement("script");
        script.src = filename + ".js";
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
        this.profileTable = new ProfileTable(this.errorReporter);
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
export function createUi(): void {
    const top = document.getElementById("top");
    if (top == null) {
        console.log("Could not find 'top' element in document");
        return;
    }
    const ui = new ProfileUI();
    top.appendChild(ui.getHTMLRepresentation());
}