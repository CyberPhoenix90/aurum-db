import * as assert from 'assert';
import { AurumDB, initializeDatabase } from '../src/aurum-db';
import { LevelUp } from 'levelup';
import { CancellationToken } from 'aurumjs';

describe('test', () => {
    let db: AurumDB;

    before(async () => {
        db = await initializeDatabase({
            path: 'unittestdb',
            integrity: {
                autoDeleteOnSetUndefined: false,
            },
        });
        await db.clear();
        const internal: LevelUp = (db as any).db;
        internal.on('put', (k, v) => console.log(`PUT: ${k} :: ${v}`));
        internal.on('del', (k) => console.log(`DEL: ${k}`));
        const original = internal.get;
        internal.get = (...args) => {
            console.log(`GET: ${args[0]}`);
            return original.call(internal, ...args);
        };
    });

    afterEach(async () => {
        await dumpDB();
        await assertDbEmpty();
        await db.clear();
    });

    describe('index', () => {
        it('create and delete index', async () => {
            assert((await db.hasIndex('test')) === false);
            await db.createIndex('test');
            assert((await db.hasIndex('test')) === true);
            assert((await db.hasLinkedCollection('test')) === false);
            assert((await db.hasOrderedCollection('test')) === false);
            await db.deleteIndex('test');
            assert((await db.hasIndex('test')) === false);
        });

        it('create nested index', async () => {
            const index = await db.createIndex('test');
            const subIndex = await index.createIndex('subIndex');

            assert((await db.hasIndex('subIndex')) === false);
            assert((await index.hasIndex('subIndex')) === true);

            await subIndex.set('hello', 'world');
            assert((await index.has('hello')) === false);
            assert((await subIndex.has('hello')) === true);

            await db.clear();

            assert((await db.hasIndex('test')) === false);
        });

        it('populate index', async () => {
            const index = await db.createIndex('test');
            await index.set('hello', 'world', 'utf8');
            assert((await index.get('hello', 'utf8')) === 'world');
            await index.set('testBinary', Buffer.from([1, 2, 3, 4]), 'binary');
            assert((await index.get('testBinary', 'binary')).equals(Buffer.from([1, 2, 3, 4])));
            await index.set('testJson', [1, 2, 3, 4], 'json');
            assert.deepStrictEqual(await index.get('testJson', 'json'), [1, 2, 3, 4]);

            assert((await db.hasIndex('test')) === true);
            await index.clear();
            assert((await db.hasIndex('test')) === true);
            await db.deleteIndex('test');
        });

        it('has key', async () => {
            const index = await db.createIndex('test');
            assert((await index.has('test')) === false);
            await index.set('test', 1, 'json');
            assert((await index.has('test')) === true);
            await index.delete('test');
            assert((await index.has('test')) === false);

            await db.deleteIndex('test');
        });

        it('observe', async () => {
            const index = await db.createIndex('test');
            const token = new CancellationToken();
            const ds = await index.observeKey('hello', token);

            assert(ds.value === undefined);
            await index.set('hello', 'world');
            assert(ds.value === 'world');
            await index.set('hello', 'world2');
            assert(ds.value === 'world2');
            await index.delete('hello');
            assert(ds.value === undefined);
            await index.set('hello', 'world');
            assert(ds.value === 'world');
            token.cancel();
            await index.delete('hello');
            assert(ds.value === 'world');

            await db.deleteIndex('test');
        });

        it('observe entire index', async () => {
            const index = await db.createIndex('test');
            const token = new CancellationToken();
            const mds = await index.observeEntireIndex(token);
            assert(Array.from(mds.keys()).length === 0);
            await index.set('hello', 'world');
            assert(Array.from(mds.keys()).length === 1);
            assert(mds.get('hello') === 'world');

            await index.set('hello', 'world2');
            assert(Array.from(mds.keys()).length === 1);
            assert(mds.get('hello') === 'world2');
            await index.delete('hello');
            assert(Array.from(mds.keys()).length === 0);
            assert(mds.has('hello') === false);
            await index.set('hello', 'world');
            assert(Array.from(mds.keys()).length === 1);
            assert(mds.get('hello') === 'world');
            token.cancel();
            await index.delete('hello');
            assert(Array.from(mds.keys()).length === 1);
            assert(mds.get('hello') === 'world');

            await db.deleteIndex('test');
        });

        /**
         * Since iterators in leveldb are snapshot based, changes to the DB during the iteration process can lead to desyncs.
         * This validates that changing the DB while iterating over it does not produce garbage results for the observer
         */
        it('observe entire index is threadsafe', async () => {
            const index = await db.createIndex('test');
            const token = new CancellationToken();

            for (let i = 0; i < 1000; i++) {
                await index.set('hello' + i, 'world');
            }

            const mdsPromise = index.observeEntireIndex(token);
            await sleep(7);
            for (let i = 0; i < 1000; i++) {
                index.set('hello' + i, 'notworld');
                index.delete('hello0');
            }
            const mds = await mdsPromise;
            for (let i = 1; i < 1000; i++) {
                assert(mds.get('hello' + i) === 'notworld');
            }
            assert(mds.has('hello0') === false);

            await db.deleteIndex('test');
        });
    });

    describe('ordered collection', () => {
        it('create and delete ordered collection', async () => {
            assert((await db.hasOrderedCollection('test')) === false);
            await db.createOrderedCollection('test');
            assert((await db.hasIndex('test')) === false);
            assert((await db.hasLinkedCollection('test')) === false);
            assert((await db.hasOrderedCollection('test')) === true);
            await db.deleteOrderedCollection('test');
            assert((await db.hasOrderedCollection('test')) === false);
            await assertDbEmpty();
        });

        it('push into ordered collection', async () => {
            const collection = await db.createOrderedCollection<number>('test');

            const promises = [];
            /**
             * Locking mechanism provides the synchronization so no await needed here for this to work. In fact it has to work without await
             */
            promises.push(collection.push(1));
            promises.push(collection.push(2));
            promises.push(collection.push(3));

            await Promise.all(promises);

            assert((await collection.length()) === 3);

            await collection.clear();
            assert((await db.hasOrderedCollection('test')) === true);
            assert((await collection.length()) === 0);

            await db.clear();

            await assertDbEmpty();
        });

        it('pop from ordered collection', async () => {
            const collection = await db.createOrderedCollection<number>('test');

            collection.push(1);
            collection.push(2);
            collection.push(3);

            assert((await collection.pop()) === 3);
            assert((await collection.pop()) === 2);

            assert((await collection.length()) === 1);

            await db.clear();

            await assertDbEmpty();
        });

        it('iterate over collection', async () => {
            const collection = await db.createOrderedCollection<number>('test');

            await collection.push(1);
            await collection.push(2);
            await collection.push(3);

            assert.deepStrictEqual(await collection.toArray(), [1, 2, 3]);
            const pairs = [
                [1, 0],
                [2, 1],
                [3, 2],
            ];

            await collection.forEach((item, index) => {
                assert.deepStrictEqual([item, index], pairs.shift());
            });
            assert(pairs.length === 0);

            await db.clear();
            await assertDbEmpty();
        });

        it('slice collection', async () => {
            const collection = await db.createOrderedCollection<number>('test');

            await collection.push(1, 2, 3, 4, 5);

            assert.deepStrictEqual(await collection.slice(0, 2), [1, 2]);
            assert.deepStrictEqual(await collection.slice(1, 1), []);
            assert.deepStrictEqual(await collection.slice(0, 1), [1]);
            assert.deepStrictEqual(await collection.slice(0, 5), [1, 2, 3, 4, 5]);

            await db.clear();
            await assertDbEmpty();
        });

        it('observe collection', async () => {
            const collection = await db.createOrderedCollection<number>('test');

            const token = new CancellationToken();
            const index = await collection.observeAt(3, token);
            const length = await collection.observeLength(token);

            assert(index.value === undefined);
            assert(length.value === 0);

            await collection.push(1, 2, 3, 4, 5);

            assert.strictEqual(index.value, 4);
            assert.strictEqual(length.value, 5);

            await db.clear();
            await assertDbEmpty();
        });
    });

    describe('linked collection', () => {
        it('create and delete linked collection', async () => {
            assert((await db.hasLinkedCollection('test')) === false);
            await db.createLinkedCollection('test');
            assert((await db.hasIndex('test')) === false);
            assert((await db.hasLinkedCollection('test')) === true);
            assert((await db.hasOrderedCollection('test')) === false);
            await db.deletedLinkedCollection('test');
            assert((await db.hasLinkedCollection('test')) === false);
            await assertDbEmpty();
        });
    });

    function sleep(time: number): Promise<void> {
        return new Promise((resolve) => setTimeout(resolve, time));
    }

    function dumpDB(): Promise<void> {
        const iter = db.iterator();
        return new Promise<void>((resolve, reject) => {
            iter.next(function cb(err, key, value) {
                if (err) {
                    iter.end(() => void 0);
                    reject(err);
                }
                if (key === undefined) {
                    iter.end(() => void 0);
                    resolve();
                } else {
                    console.log(`${key} :: ${value}`);
                    iter.next(cb);
                }
            });
        });
    }

    function assertDbEmpty(): Promise<void> {
        const iter = db.iterator();
        return new Promise((resolve, reject) => {
            iter.next((err, key, value) => {
                if (err) {
                    iter.end(() => void 0);
                    reject(err);
                }

                if (key === undefined) {
                    iter.end(() => void 0);
                    resolve();
                } else {
                    iter.end(() => void 0);
                    reject(new Error(`DB not empty found: ${key} :: ${value}`));
                }
            });
        });
    }
});
