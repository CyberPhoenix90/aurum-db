import { ArrayDataSource, DataSource } from "aurumjs";
import * as level from "level";

export type TextEncodings =
    | "utf8"
    | "hex"
    | "ascii"
    | "base64"
    | "ucs2"
    | "utf16le"
    | "utf-16le";

export type JsonEncoding = "json";

export type BinaryEncodings = "binary";

export type Encodings = TextEncodings | BinaryEncodings | JsonEncoding;

export async function initializeDatabase(config: {
    path: string;
    autoDeleteOnSetUndefined?: boolean;
}): Promise<AurumDB> {
    const dsObservers: Map<string, DataSource<any>[]> = new Map();
    const adsObservers: Map<string, ArrayDataSource<any>[]> = new Map();
    const db = await level(config.path);

    function publish(key: string, value: any) {
        if (dsObservers.has(key)) {
            for (const ds of dsObservers.get(key)) {
                ds.update(value);
            }
        }

        if (adsObservers.has(key)) {
            for (const ads of adsObservers.get(key)) {
                if (value) {
                    ads.merge(value);
                } else {
                    ads.clear();
                }
            }
        }
    }

    const self: AurumDB = {
        async observeKeyAsArray<T>(key: string): Promise<ArrayDataSource<T>> {
            const ads = new ArrayDataSource<T>(
                (await self.has(key))
                    ? await self.get<T[]>(key, "json")
                    : undefined
            );
            if (!adsObservers.has(key)) {
                adsObservers.set(key, []);
            }
            adsObservers.get(key).push(ads);

            return ads;
        },
        async observeKey<T>(
            key: string,
            encoding: Encodings = "utf8"
        ): Promise<DataSource<T>> {
            const ds = new DataSource<T>(
                (await self.has(key))
                    ? await self.get<T>(key, encoding)
                    : undefined
            );
            if (!dsObservers.has(key)) {
                dsObservers.set(key, []);
            }
            dsObservers.get(key).push(ds);

            return ds;
        },

        async has(key: string): Promise<boolean> {
            try {
                await db.get(key);
            } catch (e) {
                if (e.message.includes("Key not found in database")) {
                    return false;
                } else {
                    throw e;
                }
            }
            return true;
        },
        get<T>(key: string, encoding: Encodings = "utf8"): Promise<T> {
            return db.get(key, {
                valueEncoding: encoding,
            });
        },
        set(
            key: string,
            value: string | Buffer,
            encoding: Encodings = "utf8"
        ): Promise<void> {
            if (
                config.autoDeleteOnSetUndefined &&
                (value === undefined || value === null)
            ) {
                publish(key, undefined);
                return db.del(key);
            } else {
                publish(key, value);
                return db.put(key, value, { valueEncoding: encoding });
            }
        },
        del(key: string): Promise<void> {
            publish(key, undefined);
            return db.del(key);
        },
        clear(): Promise<void> {
            for (const dsObserver of dsObservers.values()) {
                for (const ds of dsObserver) {
                    ds.update(undefined);
                }
            }

            for (const adsObserver of adsObservers.values()) {
                for (const ads of adsObserver) {
                    ads.clear();
                }
            }
            return db.clear();
        },
    };
    return self;
}

export interface AurumDB {
    observeKeyAsArray<T>(key: string): Promise<ArrayDataSource<T>>;
    observeKey<T>(key: string, encoding?: Encodings): Promise<DataSource<T>>;
    get(key: string, encoding: BinaryEncodings): Promise<Buffer>;
    get(key: string, encoding: TextEncodings): Promise<string>;
    get<T>(key: string, encoding?: Encodings): Promise<T>;
    has(key: string): Promise<boolean>;
    set(key: string, value: Buffer, encoding: BinaryEncodings): Promise<void>;
    set(key: string, value: string, encoding: TextEncodings): Promise<void>;
    set(key: string, value: any, encoding?: Encodings): Promise<void>;
    del(key: string): Promise<void>;
    clear(): Promise<void>;
}
