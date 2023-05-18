function init() {
  registerHandler();
}

let eventBus = new EventBus('http://localhost:8080/eventbus')

function forward(command) {
  eventBus.send(
    'command-bridge/fake-aggregate',
    command
  );
}

function subscribe(aggregateId, tenantId) {
  eventBus.onerror = function (error) {
    console.error('EventBus error:', error);
  };
  eventBus.onopen = function () {
    eventBus.registerHandler(
      'state-projection/fake-aggregate/' + tenantId + '/' + aggregateId,
      null,
      function (error, message) {
        if (error) {
          console.error('Failed to register handler:', error);
        } else {
          console.log('Received message:', message.body);
        }
        document.getElementById('state').innerHTML = syntaxHighlight(JSON.stringify(message.body, null, 2));
      }
    );
    eventBus.registerHandler(
      'event-projection/fake-aggregate/' + tenantId + '/' + aggregateId,
      null,
      function (error, message) {
        if (error) {
          console.error('Failed to register handler:', error);
        } else {
          console.log('Received message:', message.body);
        }
        document.getElementById('event').innerHTML = syntaxHighlight(JSON.stringify(message.body, null, 2));
      }
    )
  }
}

function registerHandler() {
  eventBus.onerror = function (error) {
    console.error('EventBus error:', error);
  };
  eventBus.onclose = function () {
    console.log('EventBus closed');
  };
  eventBus.onopen = function () {
    console.log('EventBus opened');
    eventBus.registerHandler(
      'state-projection/fake-aggregate/default',
      null,
      function (error, message) {
        if (error) {
          console.error('Failed to register handler:', error);
        }
        document.getElementById('state').innerHTML = syntaxHighlight(JSON.stringify(message.body, null, 2));
      }
    );
    eventBus.registerHandler(
      'event-projection/fake-aggregate/default',
      null,
      function (error, message) {
        if (error) {
          console.error('Failed to register handler:', error);
        }
        document.getElementById('event').innerHTML = syntaxHighlight(JSON.stringify(message.body, null, 2));
      }
    )
  }
}

function syntaxHighlight(json) {
  json = json.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  return json.replace(
    /("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+-]?\d+)?)/g,
    match => {
      let cls = 'json-value';
      if (/^"/.test(match)) {
        if (/:$/.test(match)) {
          cls = 'json-key';
        } else {
          cls = 'json-string';
        }
      } else if (/true|false/.test(match)) {
        cls = 'json-boolean';
      } else if (/null/.test(match)) {
        cls = 'json-null';
      }
      return `<span class="${cls}">${match}</span>`;
    }
  );
}

function createFormFromJson(json) {
  var form = document.createElement("form");

  for (var fieldName in json) {
    if (json.hasOwnProperty(fieldName)) {
      var fieldData = json[fieldName];
      var fieldType = fieldData.type;
      var fieldValue = fieldData.value;

      if (fieldType === "java.util.Map") {
        // Handle map fields
        for (var key in fieldValue) {
          if (fieldValue.hasOwnProperty(key)) {
            var mapFieldValue = fieldValue[key];
            var mapFieldName = fieldName + "." + key;

            var label = document.createElement("label");
            label.innerHTML = mapFieldName + ": ";
            form.appendChild(label);

            var input = document.createElement("input");
            input.type = "text";
            input.value = mapFieldValue;
            input.name = mapFieldName;
            form.appendChild(input);
            form.appendChild(document.createElement("br"));
          }
        }
      } else if (fieldType === "java.util.Date" || fieldType === "java.time.Instant") {
        // Handle date and Instant fields
        var label = document.createElement("label");
        label.innerHTML = fieldName + ": ";
        form.appendChild(label);

        var input = document.createElement("input");
        input.type = "datetime-local";
        input.value = fieldValue.replace(" ", "T");
        input.name = fieldName;
        form.appendChild(input);
        form.appendChild(document.createElement("br"));
      } else {
        // Handle other fields
        var label = document.createElement("label");
        label.innerHTML = fieldName + ": ";
        form.appendChild(label);

        var input = document.createElement("input");
        input.type = "text";
        input.value = fieldValue;
        input.name = fieldName;
        form.appendChild(input);
        form.appendChild(document.createElement("br"));
      }
    }
  }

  document.body.appendChild(form);
}

var json = {
  "aggregateId": {
    "type": "java.lang.String",
    "value": "1234567890"
  },
  "": {
    "type": "double",
    "value": 5000.0
  },
  "isActive": {
    "type": "boolean",
    "value": true
  }
};


function generateTable() {
  var tableBody = document.querySelector("#jsonTable tbody");

  for (var i = 0; i < jsonData.length; i++) {
    var row = tableBody.insertRow(i);
    var keyCell = row.insertCell(0);
    var valueCell = row.insertCell(1);
    var actionCell = row.insertCell(2);

    keyCell.innerHTML = jsonData[i].key;
    valueCell.innerHTML = jsonData[i].value;
    actionCell.innerHTML = `<button onclick="replaceRow(${i})">Replace</button>`;
  }
}

// Function to add a new row
function addRow(jsonData) {
  var tableBody = document.querySelector("#jsonTable tbody");
  var newRow = tableBody.insertRow(jsonData.length);
  var newKeyCell = newRow.insertCell(0);
  var newValueCell = newRow.insertCell(1);
  var newActionCell = newRow.insertCell(2);

  newKeyCell.innerHTML = "<input type='text' id='newKey'>";
  newValueCell.innerHTML = "<input type='text' id='newValue'>";
  newActionCell.innerHTML = "<button onclick='saveRow()'>Save</button>";
}

// Function to replace an existing row
function replaceRow(index) {
  var tableBody = document.querySelector("#jsonTable tbody");
  var row = tableBody.rows[index];

  row.cells[0].innerHTML = "<input type='text' id='editKey'>";
  row.cells[1].innerHTML = "<input type='text' id='editValue'>";
  row.cells[2].innerHTML = "<button onclick='saveRow()'>Save</button>";
}

// Function to save the new or edited row
function saveRow() {
  var tableBody = document.querySelector("#jsonTable tbody");
  var newRow = tableBody.insertRow();

  var newKey = document.getElementById("newKey");
  var newValue = document.getElementById("newValue");

  var editKey = document.getElementById("editKey");
  var editValue = document.getElementById("editValue");

  if (newKey && newValue) {
    newRow.insertCell(0).innerHTML = newKey.value;
    newRow.insertCell(1).innerHTML = newValue.value;
  } else if (editKey && editValue) {
    var index = editKey.closest("tr").rowIndex;
    var row = tableBody.rows[index];

    row.cells[0].innerHTML = editKey.value;
    row.cells[1].innerHTML = editValue.value;
  }

  newRow.insertCell(2).innerHTML = `<button onclick="replaceRow(${tableBody.rows.length - 1})">Replace</button>`;

  // Clear input fields
  if (newKey) newKey.value = "";
  if (newValue) newValue.value = "";
  if (editKey) editKey.value = "";
  if (editValue) editValue.value = "";
}

// Generate the table when the page loads
window.onload = function () {
  generateTable();
};
