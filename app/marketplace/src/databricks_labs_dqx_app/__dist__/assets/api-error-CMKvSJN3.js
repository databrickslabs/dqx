function o(r,s){const t=r?.response?.data?.detail;if(typeof t=="string")return t;if(t&&typeof t=="object"){const e=t.summary;if(typeof e=="string")return e}return s}export{o as e};
