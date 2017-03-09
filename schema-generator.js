/*
	A script to generate a Google BigQuery-complient JSON-schema from a JSON object.

	Make sure the JSON object is complete before generating, null values will be skipped.

	References:
	https://cloud.google.com/bigquery/docs/data
	https://cloud.google.com/bigquery/docs/personsDataSchema.json
	https://gist.github.com/igrigorik/83334277835625916cd6
	... and a couple of visits to StackOverflow

	To run:
	nodejs schema-generator.js

	Output:
	schema.json
*/
var fs = require('fs')

function readJson(filename, callback) {
	fs.readFile(filename, 'utf8', function (err,data) {
	  if (err) {
		return callback(undefined);
	  }
	  callback(data);
	});
}

function writeJsonSchema(data) {
	fs.writeFile("schema.json", data, function(err) {
		if(err) {}
		console.log("The schema was saved to schema.json");
	});
}

function isObject(obj) { return obj.constructor === {}.constructor }
function isString(obj) { return obj.constructor === "test".constructor }
function isArray(obj) { return obj.constructor === [].constructor }
function isNumber(obj) { return !isNaN(parseFloat(obj)) }
function isBoolean(obj) { return typeof(obj) == "boolean" }

function getType(object) {

    if (object === null) {
        return "STRING";
    } else if (object === undefined) {
        return "undefined";
    } else if (isString(object)) {
        try {
			/* Check if string is a date string
			 The length check is present cause we only want to accept ISO strings with both time and date.
			 This also avoids a string like "2014" to be interpreted as a timestamp */
			var tryDate = new Date(object);
			if (object.length > 18 && !isNaN(tryDate.getTime())) {
				return "TIMESTAMP";
			} else {
				return "STRING";
			}
		}
		catch(err) {
			return "STRING";
		}
	} else if (isArray(object)) {
        return "Array";
    } else if (isObject(object)) {
        return "Object";
    } else if (isNumber(object)) {
		if (object.toString().indexOf('.') > 0 && object.toString().indexOf(',') > 0) {
			return "FLOAT";
		} else {
			return "INTEGER";
		}
    } else if (isBoolean(object)) {
        return "BOOLEAN";
    } else {
		return undefined;
	}
}

function createField(type, name, mode) {
	if (mode) {
		return {
			name: name,
			type: type,
			mode: mode
		};
	}

	return {
		name: name,
		type: type
	};
}

function traverse(fields, o) {
	for (i in o) {

		var name = i;
		var type = getType(o[i]);

		if (type == 'null') {
				// Skip empty fields.
		}
		else if (type == "Array")
		{
			var field = traverseArray(name, [], o[i]);
			if (field != undefined)
				fields.push(field);
        }
		else if (type == "Object")
		{
            var field = createField("record", name, undefined);
			field.fields = traverse([], o[i]);
			fields.push(field);
        } else {
			fields.push(createField(type, name, undefined));
		}
    }

	return fields;
}

function traverseArray(name, fields, o) {

	if (o.length > 0) {
		// We only want to look at the first item in the array.
		// The rest should be just like it, and will be expressed through mode=repeated.

		var firstElement = o[0];
		var type = getType(firstElement);

		if (type == 'Object') {
			// If the array has an object as the first element, we need to create a record type with more fields
			var field = createField("record", name, "repeated");
			field.fields = traverse(fields, firstElement);
			return field;
		} else {
			// If the array only has native types, we only created a simple repeated field
			var field = createField(type, name, "repeated");
			return field;
		}
	}
	return undefined;
}

// RUN
var filename = 'data.json'
var arguments = process.argv.slice(2);
if (arguments.length > 0) {
	filename = arguments[0];
}

readJson(filename, function(data) {
	if (data != undefined) {
		data = JSON.parse(data);
		fields = [];
		traverse(fields, data);

		writeJsonSchema(JSON.stringify(fields));
	} else {
		console.log("Unable to read file " + filename);
	}
});
