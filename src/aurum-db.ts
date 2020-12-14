import { AbstractIterator, AbstractIteratorOptions, AbstractBatch } from 'abstract-leveldown';
import { ArrayDataSource, CancellationToken, DataSource, MapDataSource } from 'aurumjs';
import * as level from 'level';
import { LevelUp } from 'levelup';
import * as sub from 'subleveldown';
import { BinaryEncodings, Encodings, JsonEncoding, TextEncodings } from './leveldb';

type AurumDBIntegrityConfig = {
    autoDeleteOnSetUndefined?: boolean;
};

interface AurumDBConfig {
    path: string;
    integrity?: AurumDBIntegrityConfig;
}

export async function initializeDatabase(config: AurumDBConfig): Promise<AurumDB> {
    const db = await level(config.path);

    return new AurumDB(
        db,
        config.integrity ?? {
            autoDeleteOnSetUndefined: false,
        }
    );
}

function makeSubDbId(subDbName: string, id: string): string {
    return `!${subDbName}!${id}`;
}

const META_KEY = '!!meta!!';

export class AurumDB {
    protected config: AurumDBIntegrityConfig;
    protected db: LevelUp;

    constructor(db: LevelUp, config: AurumDBIntegrityConfig) {
        this.config = config;
        this.db = db;
    }

    public iterator(options?: AbstractIteratorOptions): AbstractIterator<string, any> {
        return this.db.iterator(options);
    }

    public clear(): Promise<void> {
        return this.db.clear();
    }

    public async deleteIndex(name: string): Promise<void> {
        const index = await this.getIndex(name);
        await index.db.clear();
    }

    public async deleteOrderedCollection(name: string): Promise<void> {
        return ((await this.getOrderedCollection(name)) as any).db.clear();
    }

    public async deletedLinkedCollection(name: string): Promise<void> {
        return ((await this.getLinkedCollection(name)) as any).db.clear();
    }

    public hasIndex(name: string): Promise<boolean> {
        return this.has(makeSubDbId(name + 'index', META_KEY));
    }

    public hasOrderedCollection(name: string): Promise<boolean> {
        return this.has(makeSubDbId(name + 'ordered', META_KEY));
    }

    public hasLinkedCollection(name: string): Promise<boolean> {
        return this.has(makeSubDbId(name + 'linked', META_KEY));
    }

    public async has(key: string): Promise<boolean> {
        try {
            await this.db.get(key);
        } catch (e) {
            if (e.message.includes('Key not found in database')) {
                return false;
            } else {
                throw e;
            }
        }
        return true;
    }

    public async getIndex(name: string): Promise<AurumDBIndex> {
        if (await this.hasIndex(name)) {
            return new AurumDBIndex(sub(this.db, name + 'index'), this.config);
        } else {
            throw new Error(`Index ${name} does not exist`);
        }
    }

    public async createOrGetIndex(name: string): Promise<AurumDBIndex> {
        if (await this.hasIndex(name)) {
            return new AurumDBIndex(sub(this.db, name + 'index'), this.config);
        } else {
            return this.createIndex(name);
        }
    }

    public async getOrderedCollection<T>(name: string): Promise<AurumDBOrderedCollection<T>> {
        if (await this.hasOrderedCollection(name)) {
            return new AurumDBOrderedCollection<T>(sub(this.db, name + 'ordered'));
        } else {
            throw new Error(`Ordered collection ${name} does not exist`);
        }
    }

    public async createOrGetOrderedCollection<T>(name: string): Promise<AurumDBOrderedCollection<T>> {
        if (await this.hasOrderedCollection(name)) {
            return new AurumDBOrderedCollection<T>(sub(this.db, name + 'ordered'));
        } else {
            return this.createOrderedCollection(name);
        }
    }

    public async getLinkedCollection<T>(name: string): Promise<AurumDBLinkedCollection<T>> {
        if (await this.hasLinkedCollection(name)) {
            return new AurumDBLinkedCollection<T>(sub(this.db, name + 'linked'));
        } else {
            throw new Error(`Linked collection ${name} does not exist`);
        }
    }

    public async createOrGetLinkedCollection<T>(name: string): Promise<AurumDBLinkedCollection<T>> {
        if (await this.hasLinkedCollection(name)) {
            return new AurumDBLinkedCollection<T>(sub(this.db, name + 'linked'));
        } else {
            return this.createLinkedCollection(name);
        }
    }

    /**
     * An index is a basically a hashmap, each item is referred by key, however you can also iterate over the entire set of key values
     * Suitable use cases: Unordered lists, Hash maps, Nested Hash maps
     * Unsuitable use cases: Stacks, Ordered lists, Queues
     */
    public async createIndex(name: string): Promise<AurumDBIndex> {
        if (await this.hasIndex(name)) {
            throw new Error(`Index ${name} already exists`);
        }
        name += 'index';
        await this.db.put(makeSubDbId(name, META_KEY), new Date().toJSON(), {
            valueEncoding: 'json',
        });
        return new AurumDBIndex(sub(this.db, name), this.config);
    }
    /**
     * An ordered collection is basically an array, all items have a numerical index. Delete and Insert of items that are not the last one in the array can be very expensive. Write operations lock the entire collection due to lack of thread safetly.
     * Suitable use cases: Stacks, Append only list, Random access lists
     * Unsuitable: Queues, Hash Maps
     */
    public async createOrderedCollection<T>(name: string): Promise<AurumDBOrderedCollection<T>> {
        if (await this.hasOrderedCollection(name)) {
            throw new Error(`Ordered Collection ${name} already exists`);
        }
        name += 'ordered';
        await this.db.put(makeSubDbId(name, META_KEY), 0, {
            valueEncoding: 'json',
        });
        return new AurumDBOrderedCollection<T>(sub(this.db, name));
    }

    /**
     * A linked collection is basically a linked list. Delete and Insert of items is relatively cheap. Write operations lock only part of the collection. Iteration is fine, but random access is expensive
     * Suitable use cases: Queues, Stacks, Append only list (but ordered collection is faster for that  )
     * Unsuitable: Random access lists, Hash Maps
     */
    public async createLinkedCollection<T>(name: string): Promise<AurumDBLinkedCollection<T>> {
        if (await this.hasLinkedCollection(name)) {
            throw new Error(`Linked Collection ${name} already exists`);
        }
        name += 'linked';
        await this.db.put(makeSubDbId(name, META_KEY), 0, {
            valueEncoding: 'json',
        });
        return new AurumDBLinkedCollection<T>(sub(this.db, name));
    }
}

export class AurumDBIndex extends AurumDB {
    private totalObservers: MapDataSource<string, any>[];
    private keyObservers: Map<string, DataSource<any>[]>;

    constructor(db: LevelUp, config: AurumDBIntegrityConfig) {
        super(db, config);
        this.totalObservers = [];
        this.keyObservers = new Map();
        this.db.on('batch', (ops: AbstractBatch[]) => {
            for (const op of ops) {
                switch (op.type) {
                    case 'put':
                        this.onKeyChange(op.key, op.value);
                        break;
                    case 'del':
                        this.onKeyChange(op.key, undefined);
                        break;
                    //@ts-ignore
                    case 'clear':
                        this.onClear();
                        break;
                    default:
                        throw new Error('unhandled operation');
                }
            }
        });
        this.db.on('clear', () => {
            this.onClear();
        });

        this.db.on('put', (k, v) => {
            this.onKeyChange(k, v);
        });
        this.db.on('del', (k) => {
            this.onKeyChange(k, undefined);
        });
    }

    private onClear(): void {
        for (const mds of this.totalObservers) {
            for (const k of mds.keys()) {
                mds.delete(k);
            }
        }

        for (const dss of this.keyObservers.values()) {
            for (const ds of dss) {
                ds.update(undefined);
            }
        }
    }

    private onKeyChange(k: any, v: any): void {
        for (const mds of this.totalObservers) {
            if (v === undefined) {
                mds.delete(k);
            } else {
                mds.set(k, v);
            }
        }
        if (this.keyObservers.has(k)) {
            for (const ds of this.keyObservers.get(k)) {
                ds.update(v);
            }
        }
    }

    /**
     * Caution: While this is very useful for reactivity this has a high cost, it has to read the entire index to get started, if your index is huge this may even make your application go out of memory, to be used only with moderate sized indexes.
     * Suggested max size: 5k entries
     */
    public async observeEntireIndex<T>(cancellationToken: CancellationToken, valueEncoding?: Encodings): Promise<MapDataSource<string, T>> {
        const iter = this.iterator({
            values: false,
        });
        const result = new MapDataSource<string, T>();
        this.totalObservers.push(result);
        cancellationToken.addCancelable(() => {
            const index = this.totalObservers.indexOf(result);
            if (index !== -1) {
                this.totalObservers.splice(index, 1);
            }
        });
        let that = this;
        return new Promise<MapDataSource<string, T>>((resolve, reject) => {
            iter.next(async function cb(err, key) {
                if (err) {
                    iter.end(() => void 0);
                    reject(err);
                }
                if (key === undefined) {
                    iter.end(() => void 0);
                    resolve(result);
                } else if (!key.includes('!')) {
                    result.set(key, await that.get(key, valueEncoding));
                    iter.next(cb);
                } else {
                    iter.next(cb);
                }
            });
        });
    }

    public async observeKey<T>(key: string, cancellationToken: CancellationToken, valueEncoding?: Encodings): Promise<DataSource<T>> {
        const ds = new DataSource<T>();

        if (await this.has(key)) {
            ds.update(await this.get(key, valueEncoding));
        }

        if (!this.keyObservers.has(key)) {
            this.keyObservers.set(key, []);
        }
        this.keyObservers.get(key).push(ds);
        cancellationToken.addCancelable(() => {
            const dss = this.keyObservers.get(key);
            const index = dss.indexOf(ds);
            if (index !== -1) {
                dss.splice(index, 1);
            }
        });

        return ds;
    }

    public get(key: string, encoding: JsonEncoding): Promise<any>;
    public get(key: string, encoding: BinaryEncodings): Promise<Buffer>;
    public get(key: string, encoding: TextEncodings): Promise<string>;
    public get(key: string, encoding?: Encodings): Promise<any>;
    public get(key: string, encoding?: Encodings): Promise<any> {
        return this.db.get(key, {
            valueEncoding: encoding,
        });
    }

    public set(key: string, value: any, encoding: JsonEncoding): Promise<void>;
    public set(key: string, value: Buffer, encoding: BinaryEncodings): Promise<void>;
    public set(key: string, value: string, encoding: TextEncodings): Promise<void>;
    public set(key: string, value: any, encoding?: Encodings): Promise<void>;
    public set(key: string, value: any, encoding?: Encodings): Promise<void> {
        if (this.config.autoDeleteOnSetUndefined && (value === undefined || value === null)) {
            return this.db.del(key);
        } else {
            return this.db.put(key, value, { valueEncoding: encoding });
        }
    }
    public delete(key: string): Promise<void> {
        return this.db.del(key);
    }
    public async clear(): Promise<void> {
        const val = await this.get(META_KEY, 'json');
        await this.db.clear();
        await this.set(META_KEY, val, 'json');
    }
}

export class AurumDBLinkedCollection<T> {
    protected db: LevelUp;

    constructor(db: LevelUp) {
        this.db = db;
    }

    public clear(): Promise<void> {
        return this.db.clear();
    }
}

export class AurumDBOrderedCollection<T> {
    private totalObservers: ArrayDataSource<T>[];
    private keyObservers: Map<string, DataSource<any>[]>;
    private db: LevelUp;
    private lock: Promise<any>;

    constructor(db: LevelUp) {
        this.db = db;
        this.totalObservers = [];
        this.keyObservers = new Map();
        this.db.on('batch', (ops: AbstractBatch[]) => {
            for (const op of ops) {
                switch (op.type) {
                    case 'put':
                        this.onKeyChange(op.key, op.value);
                        break;
                    case 'del':
                        this.onKeyChange(op.key, undefined);
                        break;
                    //@ts-ignore
                    case 'clear':
                        this.onClear();
                        break;
                    default:
                        throw new Error('unhandled operation');
                }
            }
        });
        this.db.on('clear', () => {
            this.onClear();
        });

        this.db.on('put', (k, v) => {
            this.onKeyChange(k, v);
        });
        this.db.on('del', (k) => {
            this.onKeyChange(k, undefined);
        });
    }

    private onClear(): void {
        for (const dss of this.keyObservers.values()) {
            for (const ds of dss) {
                ds.update(undefined);
            }
        }
    }

    private onKeyChange(k: any, v: any) {
        if (this.keyObservers.has(k)) {
            for (const ds of this.keyObservers.get(k)) {
                ds.update(v);
            }
        }
    }

    public observeLength(cancellationToken: CancellationToken): Promise<DataSource<number>> {
        return this.observeKey(META_KEY, cancellationToken);
    }

    public observeAt(index: number, cancellationToken: CancellationToken): Promise<DataSource<T>> {
        return this.observeKey(index.toString(), cancellationToken);
    }

    private async observeKey(key: string, cancellationToken: CancellationToken): Promise<DataSource<any>> {
        await this.lock;
        const ds = new DataSource<any>();

        try {
            const v = await this.db.get(key, { valueEncoding: 'json' });
            ds.update(v);
        } catch (e) {}

        if (!this.keyObservers.has(key)) {
            this.keyObservers.set(key, []);
        }
        console.log(`listen ${key}`);
        this.keyObservers.get(key).push(ds);
        cancellationToken.addCancelable(() => {
            const dss = this.keyObservers.get(key);
            const index = dss.indexOf(ds);
            if (index !== -1) {
                dss.splice(index, 1);
            }
        });

        return ds;
    }

    /**
     * Caution: This has to read the entire collection from the database on initialization which may be slow and memory intensive. Not recommended for collections with over 5k entries
     */
    public async observeEntireCollection(cancellationToken: CancellationToken): Promise<ArrayDataSource<T>> {
        await this.lock;
        const ads = new ArrayDataSource(await this.toArray());

        this.totalObservers.push(ads);
        cancellationToken.addCancelable(() => {
            const index = this.totalObservers.indexOf(ads);
            if (index !== -1) {
                this.totalObservers.splice(index, 1);
            }
        });

        return ads;
    }

    public async length(): Promise<number> {
        await this.lock;
        return await this.db.get(META_KEY, { valueEncoding: 'json' });
    }

    public async get(index: number): Promise<T> {
        await this.lock;
        const len = await this.length();
        if (index > len) {
            throw new Error('cannot read outside of bounds of array');
        }
        return this.db.get(index, { valueEncoding: 'json' });
    }

    public async set(index: number, item: T): Promise<void> {
        await this.lock;
        const len = await this.length();
        if (index > len) {
            throw new Error('cannot write outside of bounds of array');
        }

        for (const ads of this.totalObservers) {
            ads.set(index, item);
        }
        return this.db.put(index, item, {
            valueEncoding: 'json',
        });
    }

    public async push(...items: T[]): Promise<void> {
        await this.lock;
        this.lock = new Promise(async (resolve) => {
            const len = await this.length();
            const batch = this.db.batch();
            for (let i = 0; i < items.length; i++) {
                //@ts-ignore
                batch.put(`${len + i}`, items[i], {
                    valueEncoding: 'json',
                });
            }
            //@ts-ignore
            batch.put(META_KEY, len + items.length, {
                valueEncoding: 'json',
            });
            for (const ads of this.totalObservers) {
                ads.appendArray(items);
            }

            await batch.write();
            resolve(undefined);
        });
        return this.lock;
    }

    public async slice(startIndex: number, endIndex: number): Promise<T[]> {
        await this.lock;
        const len = await this.length();
        if (startIndex > len || startIndex < 0 || endIndex > len || endIndex < 0) {
            throw new Error('cannot write outside of bounds of array');
        }
        const items = [];
        for (let i = startIndex; i < endIndex; i++) {
            items.push(await this.db.get(i, { valueEncoding: 'json' }));
        }
        return items;
    }

    public async pop(): Promise<T> {
        await this.lock;
        this.lock = new Promise(async (resolve) => {
            const len = await this.length();
            const batch = this.db.batch();

            const v = await this.db.get(len - 1, {
                valueEncoding: 'json',
            });
            //@ts-ignore
            batch.put(META_KEY, len - 1, {
                valueEncoding: 'json',
            });
            batch.del(len - 1);
            for (const ads of this.totalObservers) {
                ads.pop();
            }
            await batch.write();
            resolve(v);
        });
        return this.lock;
    }

    async clear(): Promise<void> {
        await this.lock;
        await this.db.clear();
        for (const ads of this.totalObservers) {
            ads.clear();
        }
        this.db.put(META_KEY, 0, { valueEncoding: 'json' });
    }

    async toArray(): Promise<T[]> {
        await this.lock;
        const items = [];
        const len = await this.length();
        for (let i = 0; i < len; i++) {
            items.push(await this.db.get(i, { valueEncoding: 'json' }));
        }

        return items;
    }
    async forEach(cb: (item: T, index: number) => void): Promise<void> {
        await this.lock;
        const len = await this.length();
        for (let i = 0; i < len; i++) {
            cb(
                await this.db.get(i, {
                    valueEncoding: 'json',
                }),
                i
            );
        }
    }
}
