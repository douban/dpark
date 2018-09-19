var Util = G6.Util;

var selected_call = "-1";
var selected_stage = "-1";


var stageNode = {

    draw: function (cfg, group) {
        var x = cfg.x;
        var y = cfg.y;
        var model = cfg.model;

        var sep = 10;
        var padding = 4;

        var backRect = group.addShape('rect', {
            attrs: {
                //stroke: 'blue',
                //fill: cfg.color
            }
        });

        if (model.is_output) {

            var keyGroup = group.addGroup();
            var valueGroup = group.addGroup();


            var rddGroup = group.addGroup();
            var callerGroup = group.addGroup();

            var title = group.addShape('text', {
                attrs: {
                    x: x,
                    y: y,
                    text: "stage " + model.label,
                    fill: '#888',
                    textBaseline: 'top',
                    textAlign: 'center'
                }
            });

            // console.log(model)
            var task = model.prof.counters.task;
            var p1 = task.finished / task.all;
            var p2 = (task.finished + task.running) / task.all;
            var stroke = 'l (0) 0:#00A263 ' + p1 + ':#00A263 ' + p1 + ':#E6504A ' + p2 + ':#E6504A ' + p2 + ':#0000FF';
            var splitLine1 = group.addShape('line', {
                attrs: {
                    stroke: stroke,
                    lineWidth: 10
                }
            });

            var splitLine2 = group.addShape('line', {
                attrs: {
                    stroke: stroke,
                    lineWidth: 3
                }
            });

            var i = 0;

            Util.each(model.prof_summary, function (field, _) {
                var key = field[0];
                var v = field[1];

                keyGroup.addShape('text', {
                    attrs: {
                        x: x,
                        y: y + 20 * i,
                        text: key,
                        fill: '#333',
                        textBaseline: 'top'
                    }
                });
                valueGroup.addShape('text', {
                    attrs: {
                        x: x,
                        y: y + 20 * i,
                        text: v,
                        fill: '#000',
                        textBaseline: 'top',
                        textAlign: 'right'
                    }
                });
                i += 1;
            });

            n = i;

            Util.each(model.rdds, function (field, _) {
                rddGroup.addShape('text', {
                    attrs: {
                        x: x,
                        y: y + 20 * i,
                        text: field.k,
                        fill: field.v === selected_call ? 'blue' : '#000',
                        textBaseline: 'top'
                    }
                });
                callerGroup.addShape('text', {
                    attrs: {
                        x: x,
                        y: y + 20 * i,
                        text: field.v,
                        fill: "red",
                        textBaseline: 'top',
                        textAlign: 'right'
                    }
                });
                i += 1
            });

            var titleBox = title.getBBox();

            var keyBox = keyGroup.getBBox();
            var valueBox = valueGroup.getBBox();
            var rddBox = rddGroup.getBBox();
            var callerBox = callerGroup.getBBox();

            var width = Math.max(keyBox.width + valueBox.width, rddBox.width + callerBox.width) + sep + 2 * padding;
            var height = Math.max(keyBox.height, valueBox.height) + Math.max(rddBox.height, callerBox.height) + 6 * padding + titleBox.height;


            title.translate(0, -height / 2 + padding);

            keyGroup.translate(-width / 2 + padding, -height / 2 + titleBox.height + 3 * padding);
            valueGroup.translate(width / 2 - padding, -height / 2 + titleBox.height + 3 * padding);


            rddGroup.translate(-width / 2 + padding, -height / 2 + titleBox.height + 4 * padding);
            callerGroup.translate(width / 2 - padding, -height / 2 + titleBox.height + 4 * padding);

            splitLine1.attr({
                x1: x - width / 2,
                y1: y - height / 2 + 2 * padding + titleBox.height,
                x2: x + width / 2,
                y2: y - height / 2 + 2 * padding + titleBox.height//,
                //lineWidth: 10
            });
            splitLine2.attr({
                x1: x - width / 2,
                y1: y - height / 2 + 3 * padding + titleBox.height + n * 20,
                x2: x + width / 2,
                y2: y - height / 2 + 3 * padding + titleBox.height + n * 20
            });
        } else {

            var rddGroup = group.addGroup();
            var callerGroup = group.addGroup();
            var title = group.addShape('text', {
                attrs: {
                    x: x,
                    y: y,
                    text: "stage " + model.label,
                    fill: '#888',
                    textBaseline: 'top',
                    textAlign: 'center'
                }
            });
            var i = 0;

            Util.each(model.rdds, function (field, _) {
                rddGroup.addShape('text', {
                    attrs: {
                        x: x,
                        y: y + 20 * i,
                        text: field.k,
                        fill: field.v === selected_call ? 'blue' : '#000',
                        textBaseline: 'top'
                    }
                });
                callerGroup.addShape('text', {
                    attrs: {
                        x: x,
                        y: y + 20 * i,
                        text: field.v,
                        fill: "red",
                        textBaseline: 'top',
                        textAlign: 'right'
                    }
                });
                i += 1
            });
            var titleBox = title.getBBox();
            var rddBox = rddGroup.getBBox();
            var callerBox = callerGroup.getBBox();
            var width = Math.max(rddBox.width + callerBox.width) + sep + 2 * padding;
            var height = Math.max(rddBox.height, callerBox.height) + 6 * padding + titleBox.height;
            title.translate(0, -height / 2 + padding);
            rddGroup.translate(-width / 2 + padding, -height / 2 + titleBox.height + 4 * padding);
            callerGroup.translate(width / 2 - padding, -height / 2 + titleBox.height + 4 * padding);
        }

        backRect.attr({
            x: x - width / 2,
            y: y - height / 2,
            width: width,
            height: height,
            fill: cfg.color,
            stroke: '#666',
            lineWidth: 1,
            radius: 4,
            fillOpacity: 0.10
        });
        return backRect;
    }
}