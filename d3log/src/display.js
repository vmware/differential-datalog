<meta http-equiv="Content-Type" content="text/html; charset="utf-8">
<html style="width:100%;height:100%;">
<body onload ="start()" style="width:100%;height:100%;">
<script>

var svg = false
var socket = false
var connWait = 0
var svgns = "http://www.w3.org/2000/svg"

function boundingbox(obj) {
    return obj.getBBox()
}


function softNode(parent, name, tree) {
   // look up in dom..this is n^2...there is some kind of query something something
   for (let elem of parent.children) {
       if (elem.name == name) {
           // we should be returning the old one, but to support overwrite
           // we are deleting it..fix
           elem.remove()
      }
   }
    // see if we can defer the attach...or maybe it doesnt matter
    obj = document.createElementNS(svgns, tree.kind);
    obj.name = name

   parent.appendChild(obj);
   return obj
}

function set(obj, v) {
    if (v == null) {
        obj.remove()
        return
    }

    for (var key in v) {
        var val = v[key]
	switch(key) {
        case 'children':
            for (var k in val) {
                set(softNode(obj, k, val[k]), val[k])
            }
        case 'kind':
            break
	case 'click':
            // if k is empty then remove listener
            if (!("click" in obj)) {
                   rebind_k = function(x) {
                    // val?
                       x.addEventListener("click",
                                          function (evt) {putBatch(x.click)})
                   }
                rebind_k(obj)
            } 
            obj.click = val
            break
	case 'text':
	    var textNode = document.createTextNode(val)
	    obj.appendChild(textNode);
	    break;
	default: 
	    obj.setAttributeNS(null,key,val)
	}
    }
}

// why isn't this a path walk? we should be able to set all the children, or
// some detail of an existing child
function path_set(dom, n, v) {
    if ((n["0"] == "ui") && (n["1"] == "children")){
        var name = n["1"]

        obj = softNode(dom, name, v) // right? - v is being read to determine kind
        set(obj, v)
    }
}


function clear() {
    if (svg != false) {
        svg.parentNode.removeChild(svg)
    }
    svg = document.createElementNS("http://www.w3.org/2000/svg","svg")
    svg.setAttributeNS(null, "width", "100%")
    svg.setAttributeNS(null, "height", "100%")
    document.body.appendChild(svg)
}

function send(item) {
     socket.send(JSON.stringify(item))
}

 
// this is the registration variant - maybe they are all* registration variants?
function getUpstream(path) {
     send({"read":path})
}

function putBatch(statements) {
    send(statements)
}

function websocket(url) {  
    setTimeout(function() {
	socket = new WebSocket(url)
    socket.onopen = function(evt){
        clear()
        // send({"write":{"name":{"0":"generate"}, "value":{}}})
        // getUpstream("")
    }
    socket.onmessage = function(event){
        var msg = JSON.parse(event.data)
        console.log(msg)
        // routing?
       //  path_set(svg, msg.write.name, msg.write.value)
    }
    socket.onclose = 
            function(evt){
		svg.setAttributeNS(null, "fill", "grey") 
		connWait = connWait * 2 + 1000
		if (connWait > 5000) {
		    connWait = 5000
		}
		websocket(url)
	    }
    }, connWait)
}

function start() {
    terms = document.baseURI.split(':')
    terms[0] = 'ws'
    websocket(terms.join(":"))
}
</script>
</body>        
</html>
