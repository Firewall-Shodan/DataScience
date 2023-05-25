// Função Trigger

var triggerFunction = function() {
  var pipeline = [
    { $match: { 'operationType': { $in: ['insert', 'update', 'delete'] } } },
    { $project: { 'fullDocument': 1, '_id': 0 } }
  ];

  var changeStream = db.produtos.watch(pipeline);

  while (changeStream.hasNext()) {
    var change = changeStream.next();
    var document = change.fullDocument;

    printjson(document);
  }
};



// criação do Trigger

db.createCollection('triggerProdutos');
db.triggerProdutos.insertOne({ _id: 'Id_trigger', operationType: 'insert' });

db.getCollection('triggerProdutos').watch([
  { $match: { 'operationType': 'insert'} },
  { $addFields: { 'ns': 'Tpratico.produtos' } },
  { $out: 'triggerProdutos' }
]);

db.getCollection('triggerProdutos').aggregate([{ $project: { '_id': 0, 'ns': 1 } }]).forEach(function(trigger) {
  var ns = trigger.ns;
  var parts = trigger.ns;
  if (parts.length !== 2) {
    print('Formato Inválido :', ns);
    return;
  }
  var Tpratico = parts[0];
  var produtos = parts[1];

  db.getSiblingDB(Tpratico).getCollection(produtos).watch().forEach(function(change) {
    triggerFunction();
  });
});



